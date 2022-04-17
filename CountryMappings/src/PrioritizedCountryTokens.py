from pydantic import BaseModel
from typing import Sequence


class PrioritizedCountryTokens(BaseModel):
    name: str
    prioritized_tokens: Sequence[Sequence[str]]
