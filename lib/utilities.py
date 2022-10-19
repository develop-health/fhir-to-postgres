import logging
import re


def capitalize_first_letter(text: str):
    return text[0].upper() + text[1:]

def to_snake_case(text: str) -> str:
    return re.sub(r'(?<!^)(?=[A-Z])', '_', text).lower().replace('.', '_')

def truncate_database_name(full_name: str) -> str:
    # full_name must be in snakecase format
    name = full_name
    name_parts = full_name.split('_')

    # max table name size is 63 char, abbreviate starting from third to last element until length fits
    name_part_abbreviation_index = -3
    while len(name) > 63:
        name_parts_init = name_parts[:name_part_abbreviation_index]
        # abbreviate to one charater
        name_parts_init[-1] = name_parts_init[-1][0]
        name_parts = name_parts_init + name_parts[name_part_abbreviation_index:]
        name_part_abbreviation_index -= 1
        logging.debug(f'{name_parts}')
        name = to_snake_case(''.join([capitalize_first_letter(word) for word in name_parts]))
    return name

def key_to_database_name(key: str) -> str:
    name = to_snake_case(key)
    return truncate_database_name(name)