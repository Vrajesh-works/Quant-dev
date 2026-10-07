"""Transaction cost analysis (TCA): implementation shortfall decomposition.

Implements the Perold / Wagner-Edwards taxonomy that execution desks use:

    Implementation shortfall ($) =
        spread cost + delay cost + market impact + opportunity cost

where everything is measured against the arrival (decision) price:

- spread cost   : paying through the quote,  sum q * max(px - mid, 0)
- delay cost    : market drift while working, sum q * (mid - arrival)
- market impact : modeled temporary impact of our own size (see market_impact.py)
- opportunity   : unfilled shares * max(end_price - arrival, 0)

IS (bps) = shortfall_$ / (target_shares * arrival_price) * 1e4
"""

from dataclasses import dataclass, field
from typing import Dict, List

from market_impact import ImpactParams, temporary_impact_per_share


@dataclass
class Fill:
    venue_id: str
    shares: int
    price: float   # all-in per-share execution price (incl. fees)
    mid: float     # mid-price at the time of the fill
    fee: float = 0.0  # explicit per-share fee embedded in price


@dataclass
class TCAReport:
    strategy: str
    target_shares: int
    filled_shares: int
    arrival_price: float
    gross_cash: float
    spread_cost: float
    fees: float
    delay_cost: float
    impact_cost: float      # modeled
    opportunity_cost: float
    shortfall_dollars: float
    shortfall_bps: float

    @property
    def fill_rate(self) -> float:
        return self.filled_shares / self.target_shares if self.target_shares else 0.0

    def as_dict(self) -> Dict:
        return {
            "strategy": self.strategy,
            "target": self.target_shares,
            "filled": self.filled_shares,
            "fill_rate": round(self.fill_rate, 4),
            "arrival_px": round(self.arrival_price, 4),
            "gross_cash": round(self.gross_cash, 2),
            "spread_cost": round(self.spread_cost, 2),
            "fees": round(self.fees, 2),
            "delay_cost": round(self.delay_cost, 2),
            "impact_cost_modeled": round(self.impact_cost, 2),
            "opportunity_cost": round(self.opportunity_cost, 2),
            "shortfall_$": round(self.shortfall_dollars, 2),
            "shortfall_bps": round(self.shortfall_bps, 2),
        }


def compute_tca(strategy: str,
                fills: List[Fill],
                arrival_price: float,
                end_price: float,
                target_shares: int,
                adv_map: Dict[str, float],
                impact_params: ImpactParams = None) -> TCAReport:
    """Decompose execution cost vs the arrival price."""
    impact_params = impact_params or ImpactParams()

    filled = sum(f.shares for f in fills)
    gross = sum(f.shares * f.price for f in fills)

    spread = sum(f.shares * max(f.price - f.fee - f.mid, 0.0) for f in fills)
    fees = sum(f.shares * f.fee for f in fills)
    delay = sum(f.shares * (f.mid - arrival_price) for f in fills)
    impact = sum(
        f.shares * f.mid * temporary_impact_per_share(
            f.shares, adv_map.get(f.venue_id, max(f.shares * 50, 1)), impact_params)
        for f in fills
    )
    unfilled = max(target_shares - filled, 0)
    opportunity = unfilled * max(end_price - arrival_price, 0.0)

    # Perold shortfall: what we paid vs arrival + what we missed
    shortfall = sum(f.shares * (f.price - arrival_price) for f in fills) + opportunity
    denom = target_shares * arrival_price
    bps = shortfall / denom * 10000 if denom else 0.0

    return TCAReport(
        strategy=strategy,
        target_shares=target_shares,
        filled_shares=filled,
        arrival_price=arrival_price,
        gross_cash=gross,
        spread_cost=spread,
        fees=fees,
        delay_cost=delay,
        impact_cost=impact,
        opportunity_cost=opportunity,
        shortfall_dollars=shortfall,
        shortfall_bps=bps,
    )
