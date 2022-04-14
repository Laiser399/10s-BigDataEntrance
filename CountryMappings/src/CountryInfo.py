from typing import Sequence

from pydantic import BaseModel


class CountryInfo(BaseModel):
    country_exonyms: Sequence[str]
    capital_exonyms: Sequence[str]
    country_endonyms: Sequence[str]
    capital_endonyms: Sequence[str]
    languages: Sequence[str]
