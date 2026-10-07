"""Live level-2 order books from free crypto exchange REST endpoints.

No API key needed. Each exchange becomes one "venue" in the router,
which is exactly how real fragmented equity markets look: the same
instrument quoted on several venues with different books, spreads,
depths and fees.

Unit convention: the allocator works in integer units with 100-unit
lots, so crypto sizes are normalized:
    BTC: 1 unit = 0.001 BTC     (5000 units ~= 5 BTC)
    ETH: 1 unit = 0.01  ETH     (5000 units ~= 50 ETH)

All fetching is defensive: short timeouts, per-exchange try/except, and
the caller falls back to synthetic books if fewer than 2 venues load.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
import urllib.request
import json

from allocator import Venue


@dataclass
class BookLevel:
    price: float
    size_units: int


@dataclass
class VenueBook:
    venue_id: str
    bids: List[BookLevel] = field(default_factory=list)  # best first
    asks: List[BookLevel] = field(default_factory=list)  # best first
    adv_units: float = 0.0   # 24h volume in units

    @property
    def best_bid(self) -> float:
        return self.bids[0].price if self.bids else 0.0

    @property
    def best_ask(self) -> float:
        return self.asks[0].price if self.asks else 0.0

    @property
    def mid(self) -> float:
        if self.best_bid and self.best_ask:
            return (self.best_bid + self.best_ask) / 2
        return self.best_ask or self.best_bid


SYMBOLS = {
    "BTC": {"binance": ("BTCUSDT", "BTCUSD"), "coinbase": "BTC-USD",
            "kraken": "XBTUSD", "unit_btc": 0.001},
    "ETH": {"binance": ("ETHUSDT", "ETHUSD"), "coinbase": "ETH-USD",
            "kraken": "XETHZUSD", "unit_btc": 0.01},
}

# Typical taker fees (fraction of notional) per exchange.
TAKER_FEES = {"BINANCE": 0.001, "COINBASE": 0.004, "KRAKEN": 0.0026}

_HTTP_TIMEOUT = 8


def _get_json(url: str) -> Optional[dict]:
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "sor-lab/1.0"})
        with urllib.request.urlopen(req, timeout=_HTTP_TIMEOUT) as r:
            return json.loads(r.read().decode())
    except Exception:
        return None


def _binance_book(symbol_us: str, unit: float, depth: int = 20) -> Optional[VenueBook]:
    # api.binance.us serves the US; fall back to global endpoint.
    for host, sym in (("https://api.binance.us", symbol_us[1]),
                      ("https://api.binance.com", symbol_us[0])):
        d = _get_json(f"{host}/api/v3/depth?symbol={sym}&limit={depth}")
        if d and d.get("asks"):
            break
    else:
        return None
    book = VenueBook(venue_id="BINANCE")
    for px, sz in d["asks"][:depth]:
        book.asks.append(BookLevel(float(px), int(float(sz) / unit)))
    for px, sz in d["bids"][:depth]:
        book.bids.append(BookLevel(float(px), int(float(sz) / unit)))
    t = _get_json(f"{host}/api/v3/ticker/24hr?symbol={sym}")
    if t:
        # quote volume -> base units
        qv = float(t.get("quoteVolume", 0))
        px = float(t.get("lastPrice", 0))
        if px > 0:
            book.adv_units = qv / px / unit
    return book


def _coinbase_book(product: str, unit: float) -> Optional[VenueBook]:
    d = _get_json(f"https://api.exchange.coinbase.com/products/{product}/book?level=2")
    if not d or not d.get("asks"):
        return None
    book = VenueBook(venue_id="COINBASE")
    for px, sz, _ in d["asks"][:20]:
        book.asks.append(BookLevel(float(px), int(float(sz) / unit)))
    for px, sz, _ in d["bids"][:20]:
        book.bids.append(BookLevel(float(px), int(float(sz) / unit)))
    s = _get_json(f"https://api.exchange.coinbase.com/products/{product}/stats")
    if s:
        book.adv_units = float(s.get("volume", 0)) / unit
    # Coinbase returns asks unsorted-ascending already; ensure best-first.
    book.asks.sort(key=lambda l: l.price)
    book.bids.sort(key=lambda l: -l.price)
    return book


def _kraken_book(pair: str, unit: float, depth: int = 20) -> Optional[VenueBook]:
    d = _get_json(f"https://api.kraken.com/0/public/Depth?pair={pair}&count={depth}")
    if not d or d.get("error"):
        return None
    result = d.get("result", {})
    if not result:
        return None
    key = next(iter(result))
    data = result[key]
    book = VenueBook(venue_id="KRAKEN")
    for px, sz, _ in data.get("asks", [])[:depth]:
        book.asks.append(BookLevel(float(px), int(float(sz) / unit)))
    for px, sz, _ in data.get("bids", [])[:depth]:
        book.bids.append(BookLevel(float(px), int(float(sz) / unit)))
    t = _get_json(f"https://api.kraken.com/0/public/Ticker?pair={pair}")
    if t and not t.get("error"):
        tk = next(iter(t["result"]))
        vol = float(t["result"][tk].get("v", [0, 0])[1])
        book.adv_units = vol / unit
    return book


def fetch_live_books(base: str = "BTC") -> Tuple[List[VenueBook], Dict[str, str]]:
    """Fetch L2 books for one base currency from all reachable exchanges.

    Returns (books, errors). Books are best-first ladders.
    """
    cfg = SYMBOLS.get(base.upper(), SYMBOLS["BTC"])
    unit = cfg["unit_btc"]
    books, errors = [], {}
    for name, fn, arg in (
        ("BINANCE", _binance_book, (cfg["binance"], unit)),
        ("COINBASE", _coinbase_book, (cfg["coinbase"], unit)),
        ("KRAKEN", _kraken_book, (cfg["kraken"], unit)),
    ):
        try:
            book = fn(*arg)
        except Exception as e:  # never let one venue kill the page
            book, errors[name] = None, str(e)[:80]
        if book and book.best_ask > 0 and book.asks:
            books.append(book)
        else:
            errors[name] = errors.get(name, "no book returned")
    return books, errors


def venues_from_books(books: List[VenueBook], depth_levels: int = 20) -> List[Venue]:
    """Collapse each exchange book into one Venue (top-of-book + depth)."""
    venues = []
    for b in books:
        ask_size = sum(l.size_units for l in b.asks[:depth_levels])
        fee_rate = TAKER_FEES.get(b.venue_id, 0.002)
        venues.append(Venue(
            id=b.venue_id,
            ask=b.best_ask,
            ask_size=max(ask_size, 1),
            fee=b.best_ask * fee_rate,
            rebate=0.0,
            bid=b.best_bid,
        ))
    return venues


def adv_map_from_books(books: List[VenueBook]) -> Dict[str, float]:
    return {b.venue_id: max(b.adv_units, 1.0) for b in books}


def arrival_mid(books: List[VenueBook]) -> float:
    """Consolidated mid: size-weighted across venues (a mini NBBO mid)."""
    tot_px_sz = sum(b.mid * sum(l.size_units for l in b.asks[:5]) for b in books)
    tot_sz = sum(sum(l.size_units for l in b.asks[:5]) for b in books)
    return tot_px_sz / tot_sz if tot_sz else 0.0
