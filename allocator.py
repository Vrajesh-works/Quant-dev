import math
from typing import List, Dict, Tuple
from dataclasses import dataclass

@dataclass
class Venue:
    id: str
    ask: float
    ask_size: int
    fee: float = 0.003  
    rebate: float = 0.002  

class ContKukanovAllocator:
    #Implements the Cont-Kukanov optimal order allocation algorithm
   
    
    def __init__(self, lambda_over: float, lambda_under: float, theta_queue: float):
        self.lambda_over = lambda_over
        self.lambda_under = lambda_under
        self.theta_queue = theta_queue
    
    def allocate(self, order_size: int, venues: List[Venue]) -> Tuple[List[int], float]:

        step = 100  
        splits = [[]] 
        
        for v_idx in range(len(venues)):
            new_splits = []
            for alloc in splits:
                used = sum(alloc)
                max_v = min(order_size - used, venues[v_idx].ask_size)
                
                for q in range(0, max_v + 1, step):
                    new_splits.append(alloc + [q])
            
            splits = new_splits
        
        best_cost = float('inf')
        best_split = []
        
        for alloc in splits:
            if sum(alloc) != order_size:
                continue
                
            cost = self._compute_cost(alloc, venues, order_size)
            if cost < best_cost:
                best_cost = cost
                best_split = alloc
        
        return best_split, best_cost
    
    def _compute_cost(self, split: List[int], venues: List[Venue], order_size: int) -> float:

        executed = 0
        cash_spent = 0
        
        for i, venue in enumerate(venues):
            exe = min(split[i], venue.ask_size)
            executed += exe
            cash_spent += exe * (venue.ask + venue.fee)
            
            maker_rebate = max(split[i] - exe, 0) * venue.rebate
            cash_spent -= maker_rebate
        
        underfill = max(order_size - executed, 0)
        overfill = max(executed - order_size, 0)
        
        risk_penalty = self.theta_queue * (underfill + overfill)
        cost_penalty = self.lambda_under * underfill + self.lambda_over * overfill
        
        return cash_spent + risk_penalty + cost_penalty
    
    def update_parameters(self, lambda_over: float, lambda_under: float, theta_queue: float):
        self.lambda_over = lambda_over
        self.lambda_under = lambda_under
        self.theta_queue = theta_queue

def create_venues_from_snapshot(snapshot_data: Dict) -> List[Venue]:

    venues = []
    
    venue_data = {}
    for record in snapshot_data:
        venue_id = str(record['publisher_id'])
        if venue_id not in venue_data:
            venue_data[venue_id] = {
                'ask': record['ask_px_00'],
                'ask_size': record['ask_sz_00']
            }
    
    for venue_id, data in venue_data.items():
        venues.append(Venue(
            id=venue_id,
            ask=data['ask'],
            ask_size=data['ask_size']
        ))
    
    return venues