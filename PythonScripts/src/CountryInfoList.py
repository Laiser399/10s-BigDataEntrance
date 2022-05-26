from typing import Sequence

from pydantic import BaseModel

from src import CountryInfo


class CountryInfoList(BaseModel):
    country_infos: Sequence[CountryInfo]
