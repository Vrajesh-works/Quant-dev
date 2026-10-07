"""Crypto feed: book parsing and venue construction (no network calls)."""
from crypto_feed import (BookLevel, VenueBook, adv_map_from_books, arrival_mid,
                         venues_from_books)


def make_book(vid="BINANCE", n_levels=5, base_px=50000.0):
    b = VenueBook(venue_id=vid, adv_units=1_000_000)
    for i in range(n_levels):
        b.asks.append(BookLevel(base_px + i * 0.5, 100 * (n_levels - i)))
        b.bids.append(BookLevel(base_px - 0.1 - i * 0.5, 100 * (n_levels - i)))
    return b


def test_best_bid_ask_and_mid():
    b = make_book()
    assert b.best_ask == 50000.0
    assert b.best_bid == 49999.9
    assert b.mid == (50000.0 + 49999.9) / 2


def test_venues_from_books_collapses_depth():
    books = [make_book("BINANCE"), make_book("COINBASE")]
    venues = venues_from_books(books)
    assert len(venues) == 2
    assert venues[0].id == "BINANCE"
    # ask_size = sum of all ask level sizes: 500+400+300+200+100
    assert venues[0].ask_size == 1500
    assert venues[0].bid == 49999.9
    assert venues[0].fee > 0  # taker fee priced in


def test_empty_book_properties():
    b = VenueBook(venue_id="X")
    assert b.best_ask == 0.0
    assert b.mid == 0.0


def test_arrival_mid_size_weighted():
    thin = make_book("A", base_px=50000.0)   # 1500 units
    deep = make_book("B", base_px=50100.0)   # 1500 units, worse price
    mid = arrival_mid([thin, deep])
    # equal size -> midpoint of the two mids
    assert mid == (thin.mid + deep.mid) / 2


def test_adv_map():
    books = [make_book("BINANCE")]
    m = adv_map_from_books(books)
    assert m == {"BINANCE": 1_000_000}
