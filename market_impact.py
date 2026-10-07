"""Square-root market impact model.

Practitioner-standard impact: executing Q shares against average daily
volume ADV with daily volatility sigma moves the price against the order
by roughly sigma * sqrt(Q / ADV) (Almgren et al.). Split into:

- temporary impact: decays after the trade (paying the spread / walking the book)
- permanent impact: information leakage that persists

Without impact there is no reason to split an order across venues at all,
so this is what makes the routing optimization problem real.
"""

from dataclasses import dataclass
from typing import Dict, List

from allocator import Venue


@dataclass
class ImpactParams:
    """Tunable impact coefficients."""
    k_temporary: float = 0.5   # multiplier on sigma * sqrt(q/ADV)
    k_permanent: float = 0.1    # multiplier on sigma * (q/ADV)
    daily_volatility: float = 0.02  # 2% daily vol default


def temporary_impact_per_share(shares: float, adv: float,
                               params: ImpactParams) -> float:
    """Temporary impact as a fraction of price (e.g. 0.001 = 10 bps)."""
    if shares <= 0 or adv <= 0:
        return 0.0
    return params.k_temporary * params.daily_volatility * (shares / adv) ** 0.5


def permanent_impact_per_share(shares: float, adv: float,
                               params: ImpactParams) -> float:
    """Permanent impact as a fraction of price."""
    if shares <= 0 or adv <= 0:
        return 0.0
    return params.k_permanent * params.daily_volatility * (shares / adv)


def effective_price(venue: Venue, shares: int, adv: float,
                    params: ImpactParams) -> float:
    """All-in expected execution price per share at a venue.

    ask + fee + temporary impact of our own slice. Permanent impact is
    market-wide (affects every venue) and is accounted separately in TCA.
    """
    if shares <= 0:
        return venue.ask + venue.fee
    tmp = temporary_impact_per_share(shares, adv, params)
    return (venue.ask + venue.fee) + venue.ask * tmp


def apply_impact_to_split(venues: List[Venue], split: List[int],
                          adv_map: Dict[str, float],
                          params: ImpactParams) -> Dict:
    """Reprice a venue split with market impact.

    Returns per-venue effective prices, total expected cash, and the
    impact cost broken out from the no-impact cost.
    """
    per_venue = []
    total_cash = 0.0
    no_impact_cash = 0.0
    for venue, q in zip(venues, split):
        adv = adv_map.get(venue.id, max(venue.ask_size * 50, 1))
        px = effective_price(venue, q, adv, params)
        cash = q * px
        plain = q * (venue.ask + venue.fee)
        per_venue.append({
            "venue": venue.id,
            "shares": q,
            "quoted_px": venue.ask + venue.fee,
            "effective_px": px,
            "impact_bps": (px / (venue.ask + venue.fee) - 1) * 10000 if q else 0.0,
        })
        total_cash += cash
        no_impact_cash += plain
    return {
        "per_venue": per_venue,
        "total_cash": total_cash,
        "impact_cost": total_cash - no_impact_cash,
    }


def default_adv(venue: Venue) -> float:
    """Rough ADV estimate when real volume is unknown.

    Displayed top-of-book size is a small fraction of daily volume;
    50x is a conservative practitioner rule of thumb for liquid names.
    """
    return max(venue.ask_size * 50, 1)
