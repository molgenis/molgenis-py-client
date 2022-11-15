from typing import List

import copy
import csv


def create_csv(table: List[dict], file_name: str, meta_attributes: List[str]):
    with open(file_name, "w", encoding="utf-8") as fp:
        writer = csv.DictWriter(
            fp, fieldnames=meta_attributes, quoting=csv.QUOTE_ALL, extrasaction="ignore"
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
