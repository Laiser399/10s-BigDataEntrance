import csv
from itertools import groupby
from typing import Dict, Sequence

from src import CountryInfoList, CountryInfo
from src import PrioritizedCountryTokens, PrioritizedCountryTokensList

country_infos_file_path = 'country_infos.json'


def load_country_info_list() -> CountryInfoList:
    with open(country_infos_file_path, 'r', encoding='utf-8') as input_file:
        content = input_file.read()

    return CountryInfoList.parse_raw(content)


def load_cities() -> Dict[str, Sequence[str]]:
    with open('worldcities.csv', 'r', encoding='utf-8', newline='') as input_file:
        reader = csv.reader(input_file, quotechar='\"', delimiter=',')
        next(reader)
        country_mappings = {
            'czechia': 'czech republic',
            'macedonia': 'north macedonia',
            'congo (kinshasa)': 'democratic republic of the congo',
            'congo (brazzaville)': 'republic of the congo',
            'swaziland': 'eswatini',
            'cabo verde': 'cape verde',
            'timor-leste': 'east timor',
            'sao tome and principe': 'são tomé and príncipe'
        }

        def fix_country(country_name: str) -> str:
            if country_name in country_mappings:
                return country_mappings[country_name]
            return country_name

        return {
            k: list(map(
                lambda x: tuple(set(x[1])),
                g
            ))
            for k, g in groupby(
                sorted(
                    map(
                        lambda x: (
                            fix_country(x[4].lower()),
                            (x[0].lower(), x[1].lower())
                        ),
                        reader
                    ),
                    key=lambda x: x[0]
                ),
                lambda x: x[0]
            )
        }


def map_lower(elements: Sequence[str]) -> Sequence[str]:
    return tuple(map(
        lambda x: x.lower(),
        elements
    ))


country_cities = load_cities()


def map_to_tokens(country_info: CountryInfo) -> PrioritizedCountryTokens:
    country_name = country_info.country_exonyms[0]

    cities = country_cities[country_name] if country_name in country_cities else tuple()

    return PrioritizedCountryTokens(
        name=country_name,
        prioritized_tokens=(
            map_lower(country_info.country_exonyms),
            map_lower(country_info.capital_exonyms),
            map_lower(cities),
            map_lower(country_info.country_endonyms),
            map_lower(country_info.capital_endonyms),
        )
    )


country_info_list = load_country_info_list()

res = PrioritizedCountryTokensList(
    country_list=tuple(map(
        map_to_tokens,
        country_info_list.country_infos
    ))
)

with open('prioritized_tokens.json', 'w') as output_file:
    output_file.write(res.json())
