from enum import Enum
from dataclasses import dataclass, field
from http.cookiejar import CookiePolicy
from typing import List

import copy
import csv
import json


class BlockAll(CookiePolicy):
    def return_ok(self, cookie, request):
        """Cookie handling"""
        pass

    def set_ok(self, cookie, request):
        return False


class ImportDataAction(Enum):
    """Enum of MOLGENIS import actions"""

    ADD = "add"
    ADD_UPDATE_EXISTING = "add_update_existing"
    UPDATE = "update"
    ADD_IGNORE_EXISTING = "add_ignore_existing"


class ImportMetadataAction(Enum):
    """Enum of MOLGENIS import metadata actions"""

    ADD = "add"
    UPDATE = "update"
    UPSERT = "upsert"
    IGNORE = "ignore"


class MolgenisRequestError(Exception):
    def __init__(self, error, response=False):
        self.message = error
        if response:
            self.response = response


@dataclass(frozen=True)
class Headers:
    """
    This class is responsible for creating 'x-molgenis-token' headers for the current session
    """

    token: str
    token_header: dict = field(init=False)
    ct_token_header: dict = field(init=False)

    def __post_init__(self):
        """Create an 'x-molgenis-token' header for the current session and a
        'Content-Type: application/json' header"""
        if self.token:
            object.__setattr__(self, "token_header", {"x-molgenis-token": self.token})
            object.__setattr__(self, "ct_token_header",
                               {"x-molgenis-token": self.token, "Content-Type": "application/json"})
        else:
            object.__setattr__(self, "token_header", {})
            object.__setattr__(self, "ct_token_header", {})


def create_csv(table: List[dict], file_name: str, meta_attributes: List[str]):
    with open(file_name, "w", encoding="utf-8") as fp:
        writer = csv.DictWriter(
            fp, fieldnames=meta_attributes, quoting=csv.QUOTE_ALL
        )
        writer.writeheader()
        for row in table:
            for key, value in row.items():
                if isinstance(value, list):
                    row[key] = ",".join(value)
            writer.writerow(row)


def merge_two_dicts(x: dict, y: dict) -> dict:
    """Given two dicts, merge them into a new dict as a shallow copy."""
    z = x.copy()
    z.update(y)
    return z


def raise_exception(ex):
    """Raises an exception with error message from molgenis"""
    message = ex.args[0]
    if ex.response.content:
        try:
            error = json.loads(ex.response.content.decode("utf-8"))['errors'][0]['message']
        except ValueError:  # Cannot parse JSON
            error = ex.response.content
        error_msg = '{}: {}'.format(message, error)
        raise MolgenisRequestError(error_msg, ex.response)
    else:
        raise MolgenisRequestError('{}'.format(message))


def remove_one_to_manys(rows: List[dict], meta: dict) -> List[dict]:
    """
    Removes all one-to-manys from a list of rows based on the table's metadata. Removing
    one-to-manys is necessary when adding new rows. Returns a copy so that the original
    rows are not changed in any way.
    """
    one_to_manys = []
    for attribute in meta["attributes"].keys():
        if meta["attributes"][attribute]["fieldType"] == "ONE_TO_MANY":
            print(attribute)
            one_to_manys.append(attribute)
    copied_rows = copy.deepcopy(rows)
    for row in copied_rows:
        for one_to_many in one_to_manys:
            row.pop(one_to_many, None)
    return copied_rows


def set_urls(url: str):
    """ Sets the root and API URLs.
    Historically, the URL had to be passed with '/api' at the end. This method is for backwards compatibility and
    allows for URLs both with and without the '/api' postfix.
    """
    root_url = url.rstrip('/').rstrip('/api') + '/'
    api_url = root_url + 'api/'

    return api_url, root_url
