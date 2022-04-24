import csv
from itertools import chain
from typing import Optional

from src import PrioritizedCountryTokensList


def fix_tokens_list(tokens_list: PrioritizedCountryTokensList):
    handmade_aliases = {
        'united kingdom': ['uk'],
        'united states': ['usa', 'new york', 'us', 'new jersey', 'denver, co', 'ohio'],
        'ukraine': ['украина', 'kiev'],
        'china': ['hong kong', 'hongkong', 'hong kong, hong kong'],
        'india': ['pune', 'delhi'],
        'canada': ['toronto'],
        'australia': ['sydney'],
    }

    for country_tokens in tokens_list.country_list:
        lower_country_name = country_tokens.name.lower()
        if lower_country_name not in handmade_aliases:
            continue

        country_tokens.prioritized_tokens = tuple(chain(
            country_tokens.prioritized_tokens,
            [handmade_aliases[lower_country_name]]
        ))


class Matcher:
    def __init__(self, tokens_list: PrioritizedCountryTokensList):
        self._tokens_list = tokens_list
        self._lowest_priority = max(map(
            lambda x: len(x.prioritized_tokens) - 1,
            tokens_list.country_list
        ))

    def match(self, value: str) -> Optional[str]:
        value = value.lower()

        for i in range(self._lowest_priority + 1):
            for country_tokens in self._tokens_list.country_list:
                if i >= len(country_tokens.prioritized_tokens):
                    continue

                for token in country_tokens.prioritized_tokens[i]:
                    if token in value:
                        return country_tokens.name
        return None


def create_matcher(tokens_list_file_path: str) -> Matcher:
    with open(tokens_list_file_path, 'r', encoding='utf-8') as input_file:
        content = input_file.read()

    tokens_list = PrioritizedCountryTokensList.parse_raw(content)
    fix_tokens_list(tokens_list)
    return Matcher(tokens_list)


def merge(reader, writer, matcher: Matcher):
    success, fail = 0, 0
    for row in reader:
        assert len(row) == 2

        weird_location = row[0]

        matched_country = matcher.match(weird_location)

        if matched_country:
            success += 1
            writer.writerow([weird_location, matched_country])
        else:
            fail += 1
            print(f'{row[1]}\t{row[0]} - Fail')
    print(f'success: {success}')
    print(f'fail: {fail}')


if __name__ == '__main__':
    input_file_path = '000000_0'
    output_file_path = 'location_mappings.csv'
    tokens_list_file_path = 'prioritized_tokens.json'

    matcher = create_matcher(tokens_list_file_path)
    with open(input_file_path, 'r', encoding='utf-8', newline='') as input_file:
        with open(output_file_path, 'w', encoding='utf-8', newline='') as output_file:
            reader = csv.reader(input_file, delimiter='\t', escapechar='\\')
            writer = csv.writer(output_file, delimiter='\t', escapechar='\\')
            merge(reader, writer, matcher)
