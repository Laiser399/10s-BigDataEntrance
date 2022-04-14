import re
from functools import reduce
from typing import Sequence

from bs4 import BeautifulSoup
from bs4.element import Tag

from .CountryInfo import CountryInfo
from .CountryInfoList import CountryInfoList


class CountryInfoExtractor:
    _ignore_element_patterns = [
        re.compile(r'^\[\d+]$'),
        re.compile(r'^\d+$'),
        re.compile(r'^or$'),
        re.compile(r'^formerly$'),
    ]

    _clear_patterns = [
        re.compile(r'\(.*\)'),
        re.compile(r'[,\[\]()/\\]'),
    ]

    def __init__(self, bs: BeautifulSoup):
        self._bs = bs

    def extract(self) -> CountryInfoList:
        tables = self._bs.select('.mw-parser-output > table.wikitable')

        return CountryInfoList(
            country_infos=list(map(
                self._extract_country_info,
                reduce(
                    lambda a, b: a + b,
                    [self._extract_rows(table) for table in tables]
                )
            ))
        )

    def _extract_rows(self, table: Tag) -> Sequence[Tag]:
        return table.select('tr:not(:first-child)')

    def _extract_country_info(self, row: Tag) -> CountryInfo:
        elements = row.select('td')

        assert len(elements) == 5

        country_exonyms = self._extract_column_elements(elements[0])
        capital_econyms = self._extract_column_elements(elements[1])
        country_endonyms = self._extract_column_elements(elements[2])
        capital_endonyms = self._extract_column_elements(elements[3])
        languages = self._extract_column_elements(elements[4])

        assert len(country_exonyms) == 1
        assert len(country_endonyms) >= 1
        assert len(languages) >= 1

        return CountryInfo(
            country_exonyms=country_exonyms,
            capital_exonyms=capital_econyms,
            country_endonyms=country_endonyms,
            capital_endonums=capital_endonyms,
            languages=languages
        )

    def _extract_column_elements(self, column: Tag) -> Sequence[str]:
        return list(filter(
            lambda x: len(x) > 0 and not self._is_ignore_column_element(x),
            map(
                lambda x: self._clear_column_element(x.text).strip(),
                column.children
            )
        ))

    def _is_ignore_column_element(self, value: str) -> bool:
        for pattern in self._ignore_element_patterns:
            if pattern.match(value):
                return True
        return False

    def _clear_column_element(self, value: str) -> str:
        return reduce(
            lambda x, p: p.sub('', x),
            self._clear_patterns,
            value
        )
