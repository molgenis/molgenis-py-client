from typing import List, Optional


def build_api_url(base_url: str, possible_options: dict):
    """This function builds the api url for the get request, converting the api v1 compliant operators to v2
    operators to enable backwards compatibility of the python api when switching to api v2"""
    operators = []
    for option, option_value in possible_options.items():
        operator = None
        if option == 'q' and option_value:
            operator = process_query(option_value, option)
        elif option == 'sort':
            operator = process_sort(option_value)
        elif option == 'attrs':
            operator = merge_attrs(option_value)
        elif option_value and not (option == 'num' and option_value == 100):
            operator = '{}={}'.format(option, option_value)

        if operator:
            operators.append(operator)

    url = '{}?{}'.format(base_url, '&'.join(operators))

    if url == base_url + '?':
        return base_url
    else:
        return url

def process_query(option_value, option: str) -> str:
    """Add query to operators and raise exception when query value is invalid"""
    if type(option_value) == list:
        raise TypeError('Please specify your query in the RSQL format.')
    else:
        return '{}={}'.format(option, option_value)


def process_sort(option_value: List[str]) -> str:
    """Converts the sort and sort order to a sort attribute compatible with the REST API v2"""
    if option_value[0] and not option_value[1]:
        return 'sort=' + option_value[0]
    elif option_value[0] and option_value[1]:
        return 'sort={}:{}'.format(option_value[0], option_value[1])


def split_if_not_none(operator: Optional[str]) -> List[str]:
    """Returns empty list if operator is None, else splits the operator string by comma to return a list"""
    return operator.split(',') if operator else []


def merge_attrs(attr_expands: List[Optional[str]]) -> str:
    """Converts the attrs and expands to an attr attribute compatible with the REST API v2"""
    # Make a list of attrs and expands
    attrs = split_if_not_none(attr_expands[0])
    expands = split_if_not_none(attr_expands[1])
    # If only expands is specified, all attributes should be returned, so add a wildcard to the list
    if len(attrs) == 0 and len(expands) > 0:
        attrs.append('*')
    # Get a set of all unique attributes (expands and attributes merged)
    unique_attrs = set(attrs + expands)
    # Iterate over all unique attributes and expand by adding (*) if the attributes is in the expands list
    attrs_operator = [attr + '(*)' if attr in expands else attr for attr in unique_attrs]
    # If there is an attrs operator, return it with its prefix and comma separated
    if attrs_operator:
        return 'attrs={}'.format(','.join(attrs_operator))
