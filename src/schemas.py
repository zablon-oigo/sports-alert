from pydantic import BaseModel 
from typing import List, Optional 

class SubscriptionCreate(BaseModel):
    email: str 
    phone: Optional[str]
    sports: List[str]
    teams: List[str]
    notify_on: List[str]
    