from typing import Sequence

from pydantic import BaseModel

from . import PrioritizedCountryTokens


class PrioritizedCountryTokensList(BaseModel):
    country_list: Sequence[PrioritizedCountryTokens]
