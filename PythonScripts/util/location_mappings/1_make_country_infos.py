from typing import Sequence

from bs4 import BeautifulSoup

from src import CountryInfoList, CountryInfoExtractor


def get_valid_names() -> Sequence[str]:
    fixed_names = {
        'Myanmar (formerly Burma)': 'Myanmar',
        'Sao Tome and Principe': 'São Tomé and Príncipe',
        'Bahamas': 'The Bahamas',
        'Czechia (Czech Republic)': 'Czech Republic',
        'Gambia': 'The Gambia',
        'Cabo Verde': 'Cape Verde',
        'Holy See': 'Vatican City',
        'Timor-Leste': 'East Timor',
        'Micronesia': 'Federated States of Micronesia',
        'Palestine State': 'Palestine',
        'Eswatini (fmr. "Swaziland")': 'Eswatini',
        'United States of America': 'United States',
        'Congo (Congo-Brazzaville)': 'Republic of the Congo',
    }

    with open('input/valid_countries.html', 'r', encoding='utf-8') as input_file:
        content = input_file.read()
    bs = BeautifulSoup(content, 'lxml')
    return list(map(
        lambda x: fixed_names[x] if x in fixed_names else x,
        map(
            lambda x: x.text.strip(),
            bs.select('table.table tbody tr td:nth-child(2)')
        )
    ))


def merge_with_valid(country_info_list: CountryInfoList) \
        -> (CountryInfoList, Sequence[str], Sequence[str]):
    """
    :param country_info_list:
    :return: (valid country info list, invalid country names, not found valid country names)
    """
    valid_names = set(get_valid_names())
    country_info_list = CountryInfoList(
        country_infos=list(filter(
            lambda x: x.country_exonyms[0] in valid_names,
            country_info_list.country_infos
        ))
    )

    left_names = set(map(
        lambda x: x.country_exonyms[0],
        country_info_list.country_infos
    ))
    invalid_country_names = list(filter(
        lambda x: x not in valid_names,
        left_names
    ))
    not_found_country_names = list(filter(
        lambda x: x not in left_names,
        valid_names
    ))

    return country_info_list, invalid_country_names, not_found_country_names


with open('input/wiki_counties.html', 'r', encoding='utf-8') as input_file:
    content = input_file.read()

bs = BeautifulSoup(content, 'lxml')
extractor = CountryInfoExtractor(bs)
country_info_list = extractor.extract()

country_info_list, invalid, not_found = merge_with_valid(country_info_list)

assert len(invalid) == 0
assert len(country_info_list.country_infos) == 195

with open('output/country_infos.json', 'w', encoding='utf-8') as output_file:
    output_file.write(country_info_list.json(indent='    '))
