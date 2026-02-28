from pydantic import BaseModel
from typing import Optional

class SubmitRequest(BaseModel):
    sample_vcf: str
    node: Optional[str] = None
