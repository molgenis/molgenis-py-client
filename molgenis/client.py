
import json
import os
import tempfile
from pathlib import Path
from time import sleep
from typing import List, Union, Any
from urllib.parse import quote_plus, urlparse, parse_qs
from zipfile import ZipFile

import requests

from molgenis.api_support import (BlockAll,
                                  Headers,
                                  ImportDataAction,
                                  ImportMetadataAction)

from molgenis.errors import MolgenisRequestError, raise_exception
import molgenis.query_utils as query_utils
import molgenis.utils as utils


class Session:
    """Representation of a session with the MOLGENIS REST API.
    Usage:

    >>> session = Session('http://localhost:8080/')
    >>> session.login('user', 'password')
    >>> session.get('Person')
    """

    def __init__(self, url: str = "http://localhost:8080/", token: str = None):
        """Constructs a new Session.
        Args:
        url -- URL of the REST API. Should be of form 'http[s]://<molgenis server>[:port]/'
        token -- authentication token if you are already logged in

        Examples:
        >>> session = Session('http://localhost:8080/')
        """
        self._set_urls(url)
        self._session = requests.Session()
        self._session.cookies.policy = BlockAll()
        self._token = token
        self._headers = Headers(token=self._token)

    def login(self, username: str, password: str):
        """Logs in a user and stores the acquired token in this Session object.

        Args:
        username -- username for a registered molgenis user
        password -- password for the user
        """
        response = self._session.post(self._api_url + "v1/login",
                                      data=json.dumps({"username": username,
                                                       "password": password}),
                                      headers={"Content-Type": "application/json"})
        try:
            response.raise_for_status()
        except requests.RequestException as ex:
            raise_exception(ex)

        self._token = response.json()['token']
        self._headers = Headers(token=self._token)

    def logout(self):
        """Logs out the current token."""
        response = self._session.post(self._api_url + "v1/logout",
                                      headers=self._headers.token_header)
        try:
            response.raise_for_status()
        except requests.RequestException as ex:
            raise_exception(ex)

        self._token = None

    def get_by_id(self, entity: str, id_: str, attributes: str = None,
                  expand: str = None, uploadable: bool = False) -> dict:
        """Retrieves a single entity row from an entity repository.

        Args:
        entity -- fully qualified name of the entity
        id_ -- the value for the idAttribute of the entity
        attributes -- The list of attributes to retrieve (comma separated)
        expand -- the attributes to expand, string with commas to separate multiple attributes.
        uploadable -- when true the output of the REST Client will be changed such that it can be uploaded again

        Examples:
        >>> session = Session('http://localhost:8080/api/')
        >>> session.get(entity='Person', id_='John', expand='name,age')
        """
        possible_options = {'attrs': [attributes, expand]}

        url = query_utils.build_api_url(self._api_url + "v2/" + quote_plus(entity) + '/' + quote_plus(id_), possible_options)
        response = self._session.get(url, headers=self._headers.token_header)

        try:
            response.raise_for_status()
        except requests.RequestException as ex:
            raise_exception(ex)

        result = response.json()
        response.close()

        if uploadable:
            result = self.to_upload_format(entity, [result])[0]

        return result

    def get(self,
            entity: str,
            q: str = None,
            attributes: str = None,
            num: int = None,
            batch_size: int = 100,
            start: int = 0,
            sort_column: str = None,
            sort_order: str = None,
            raw: bool = False,
            expand: str = None,
            uploadable: bool = False) -> Union[List[dict], dict]:
        """Retrieves all entity rows from an entity repository.

        Args:
        entity -- fully qualified name of the entity
        q -- query in rsql format, see our RSQL documentation for details
            (https://molgenis.gitbooks.io/molgenis/content/developer_documentation/ref-RSQL.html)
        attributes -- The list of attributes to retrieve (as comma-separated string)
        expand -- the attributes to expand (as comma-separated string)
        num -- the maximum amount of entity rows to retrieve
        batch_size - the amount of entity rows to retrieve per time (max. 10.000)
        start -- the index of the first row to retrieve (zero indexed)
        sortColumn -- the attribute to sort on
        sortOrder -- the order to sort in
        raw -- when true, the complete REST response will be returned, rather than the data items alone
        uploadable -- when true the output of the REST Client will be changed such that it can be uploaded again

        Examples:
        >>> session = Session('http://localhost:8080/api/')
        >>> session.get('Person')
        >>> session.get(entity='Person', q='name=="Henk"', attributes='name,age'])
        >>> session.get(entity='Person', expand='mother,father'])
        >>> session.get(entity='Person', sort_column='age', sort_order='desc')
        >>> session.get('Person', raw=True)
        """
        if not sort_column:  # Ensure correct ordering for batched retrieval for old Molgenis instances
            sort_column = self.get_entity_meta_data(entity)['idAttribute']

        batch_start = start
        items = []
        while not num or len(items) < num:  # Keep pulling in batches
            response = self._get_batch(
                entity=entity,
                q=q,
                attributes=attributes,
                batch_size=batch_size,
                start=batch_start,
                sort_column=sort_column,
                sort_order=sort_order,
                raw=True,
                expand=expand)
            if raw:
                return response  # Simply return the first batch response JSON
            else:
                items.extend(response['items'])

            if 'nextHref' in response:  # There is more to fetch
                decomposed_url = urlparse(response['nextHref'])
                query_part_url = parse_qs(decomposed_url.query)
                batch_start = query_part_url['start'][0]
            else:
                break  # We caught them all

        if num:  # Truncate items
            items = items[:num]

        if uploadable:
            items = self.to_upload_format(entity, items)

        return items

    def _get_batch(self,
                   entity: str,
                   q: str = None,
                   attributes: str = None,
                   batch_size: int = 100,
                   start: int = 0,
                   sort_column: str = None,
                   sort_order: str = None,
                   raw: bool = False,
                   expand: str = None) -> Union[List[dict], dict]:
        """ Retrieves a batch of entity rows from an entity repository. """
        possible_options = {'q': q,
                            'attrs': [attributes, expand],
                            'num': batch_size,
                            'start': start,
                            'sort': [sort_column, sort_order]}

        url = query_utils.build_api_url(self._api_url + "v2/" + quote_plus(entity), possible_options)
        response = self._session.get(url, headers=self._headers.token_header)

        try:
            response.raise_for_status()
        except requests.RequestException as ex:
            raise_exception(ex)

        if raw:
            return response.json()
        else:
            return response.json()["items"]

    def add(self, entity: str, data: dict = None, files: dict = None, **kwargs) -> str:
        """Adds a single entity row to an entity repository.

        Args:
        entity -- fully qualified name of the entity
        files -- dictionary containing file attribute values for the entity row.
        The dictionary should for each file attribute map the attribute name to a tuple containing the file name and an
        input stream.
        data -- dictionary mapping attribute name to non-file attribute value for the entity row, gets merged with the
        kwargs argument
        **kwargs -- keyword arguments get merged with the data argument

        Examples:
        >>> session = Session('http://localhost:8080/api/')
        >>> session.add('Person', firstName='Jan', lastName='Klaassen')
        >>> session.add('Person', {'firstName': 'Jan', 'lastName':'Klaassen'})

        You can have multiple file type attributes.

        >>> session.add('Plot', files={'image': ('expression.jpg', open('~/first-plot.jpg','rb')),\
        'image2': ('expression-large.jpg', open('/Users/me/second-plot.jpg', 'rb'))}, data={'name':'IBD-plot'})
        """
        if not data:
            data = {}
        if not files:
            files = {}

        response = self._session.post(self._api_url + "v1/" + quote_plus(entity),
                                      headers=self._headers.token_header,
                                      data=utils.merge_two_dicts(data, kwargs),
                                      files=files)
        try:
            response.raise_for_status()
        except requests.RequestException as ex:
            raise_exception(ex)

        return response.headers["Location"].split("/")[-1]

    def add_all(self, entity: str, entities: List[dict]) -> List[str]:
        """Adds multiple entity rows to an entity repository."""
        response = self._session.post(self._api_url + "v2/" + quote_plus(entity),
                                      headers=self._headers.ct_token_header,
                                      data=json.dumps({"entities": entities}))

        try:
            response.raise_for_status()
        except requests.RequestException as ex:
            raise_exception(ex)

        return [resource["href"].split("/")[-1] for resource in response.json()["resources"]]

    def update_one(self, entity: str, id_: str, attr: str, value: Any) -> requests.Response:
        """Updates one attribute of a given entity in a table with a given value"""
        response = self._session.put(self._api_url + "v1/" + quote_plus(entity) + "/" + id_ + "/" + attr,
                                     headers=self._headers.ct_token_header,
                                     data=json.dumps(value))

        try:
            response.raise_for_status()
        except requests.RequestException as ex:
            raise_exception(ex)

        return response

    def update_all(self, entity: str, entities: List[dict]):
        """Updates multiple entities."""
        response = self._session.put(
            self._api_url + "v2/" + quote_plus(entity),
            headers=self._headers.ct_token_header,
            data=json.dumps({"entities": entities}),
        )

        try:
            response.raise_for_status()
        except requests.RequestException as ex:
            raise_exception(ex)

        return response

    def upsert(self, entity_type_id: str, entities: List[dict]):
        """
        Upserts entities in an entity type.
        @param entity_type_id: the id of the entity type to upsert to
        @param entities: the entities to upsert
        """
        # Get the existing identifiers
        meta = self.get_entity_meta_data(entity_type_id)
        id_attr = meta["idAttribute"]
        existing_entities = self.get(entity_type_id, batch_size=10000, attributes=id_attr)
        existing_ids = {entity[id_attr] for entity in existing_entities}

        # Based on the existing identifiers, decide which rows should be added/updated
        add = list()
        update = list()
        for entity in entities:
            if id_attr in entity and entity[id_attr] in existing_ids:
                    update.append(entity)
            else:
                add.append(entity)

        # Sanitize data: rows that are added should not contain one_to_manys
        add = utils.remove_one_to_manys(add, meta)

        # Do the adds and updates separately
        self.add_all(entity_type_id, add)
        self.update_all(entity_type_id, update)

    def delete(self, entity: str, id_: str = None) -> requests.Response:
        """Deletes a single entity row or all rows (if id_ not specified) from an entity repository."""
        url = self._api_url + "v1/" + quote_plus(entity)
        if id_:
            url = url + "/" + quote_plus(id_)

        response = self._session.delete(url, headers=self._headers.token_header)
        try:
            response.raise_for_status()
        except requests.RequestException as ex:
            raise_exception(ex)

        return response

    def delete_list(self, entity: str, entities: List[str]) -> requests.Response:
        """Deletes multiple entity rows to an entity repository, given a list of id's."""
        response = self._session.delete(self._api_url + "v2/" + quote_plus(entity),
                                        headers=self._headers.ct_token_header,
                                        data=json.dumps({"entityIds": entities}))
        try:
            response.raise_for_status()
        except requests.RequestException as ex:
            raise_exception(ex)

        return response

    def get_entity_meta_data(self, entity: str) -> dict:
        """Retrieves the metadata for an entity repository."""
        response = self._session.get(self._api_url + "v1/" + quote_plus(entity) + "/meta?expand=attributes",
                                     headers=self._headers.token_header)
        try:
            response.raise_for_status()
        except requests.RequestException as ex:
            raise_exception(ex)

        return response.json()

    def get_attribute_meta_data(self, entity: str, attribute: str) -> dict:
        """Retrieves the metadata for a single attribute of an entity repository."""
        response = self._session.get(self._api_url + "v1/" + quote_plus(entity) + "/meta/" + quote_plus(attribute),
                                     headers=self._headers.token_header)
        try:
            response.raise_for_status()
        except requests.RequestException as ex:
            raise_exception(ex)

        return response.json()

    def get_meta(self, entity_type_id: str, expand: bool = False, abstract: bool = False):
        """Similar to get_entity_meta_data(), but uses the newer Metadata API instead
        of the REST API V1.
        If expand is true, the metadata of the ref entities will be returned also.
        If abstract is true, the metadata of the parent entity will be returned also.
        """
        response = self._session.get(
            self._api_url + "metadata/" + quote_plus(entity_type_id) + "?flattenAttributes="+str(abstract),
            headers=self._headers.token_header,
        )

        try:
            response.raise_for_status()
        except requests.RequestException as ex:
            raise_exception(ex)

        meta = response.json()["data"]

        if expand:
            for item in meta["attributes"]["items"]:
                if "refEntityType" in item["data"]:
                    ref_url = item["data"]["refEntityType"]["self"]
                    ref_entity = ref_url[ref_url.rindex("/"):].replace("/", "")
                    item["data"]["refEntityType"] = self.get_meta(ref_entity, abstract=True)

        return meta

    def to_upload_format(self, entity_type_id: str, rows: List[dict], ) -> List[dict]:
        """
        Changes the output of the REST Client such that it can be uploaded again:
        1. Non-data fields are removed (_href and _meta).
        2. Reference objects are removed and replaced with their identifiers.
        """
        meta = self.get_meta(entity_type_id, expand=True, abstract=True)
        upload_format = []
        ref_ids = {}
        # Get the idAttributes of the refEntities
        for attr in meta["attributes"]["items"]:
            if "refEntityType" in attr["data"]:
                for ref_attr in attr["data"]["refEntityType"]["attributes"]["items"]:
                    if ref_attr["data"]["idAttribute"] is True:
                        ref_ids[attr["data"]["name"]] = ref_attr["data"]["name"]

        for row in rows:
            # Remove non-data fields
            row.pop("_href", None)
            row.pop("_meta", None)

            for attr in row:
                if type(row[attr]) is dict:
                    # Change xref dicts to id
                    ref = row[attr][ref_ids[attr]]
                    row[attr] = ref
                elif type(row[attr]) is list and len(row[attr]) > 0:
                    # Change mref list of dicts to list of ids
                    mref = [ref[ref_ids[attr]] for ref in row[attr]]
                    row[attr] = mref

            upload_format.append(row)
        return upload_format

    def upload_zip(self,
                   meta_data_zip: str,
                   data_action: ImportDataAction = ImportDataAction.ADD,
                   metadata_action: ImportMetadataAction = ImportMetadataAction.UPSERT,
                   asynchronous: bool = True) -> str:
        """Uploads a given zip with data and/or metadata
        If asynchronous is True it does not wait till the upload is finished.
        Options for metadata_action are: [ADD, UPDATE, UPSERT, IGNORE]
        Options for data_action are: [ADD, ADD_UPDATE_EXISTING, UPDATE, ADD_IGNORE_EXISTING]
        """

        params = {"action": data_action.value, "metadataAction": metadata_action.value}
        with open(os.path.abspath(meta_data_zip), 'rb') as zip_file:
            files = {'file': zip_file}
            url = self._root_url + 'plugin/importwizard/importFile'
            response = requests.post(url, headers=self._headers.token_header, files=files, params=params)
        try:
            response.raise_for_status()
        except requests.RequestException as ex:
            raise_exception(ex)

        if not asynchronous:
            self._await_import_job(response.text.split("/")[-1])

        return response.content.decode("utf-8")

    def import_data(self, data: dict, data_action: ImportDataAction, metadata_action: ImportMetadataAction):
        with tempfile.TemporaryDirectory() as tmpdir:
            archive = self._create_emx_archive(data, tmpdir)
            self.upload_zip(
                archive,
                data_action,
                metadata_action,
                asynchronous=False
            )

    def _create_emx_archive(self, data: dict, directory: str) -> Path:
        archive_name = f"{directory}/archive.zip"
        with ZipFile(archive_name, "w") as archive:
            for table_name in data.keys():
                meta_attributes = [
                    attr["data"]["name"] for attr in
                    self.get_meta(entity_type_id=table_name)["attributes"]["items"]
                ]
                file_name = f"{table_name}.csv"
                file_path = f"{directory}/{file_name}"
                utils.create_csv(data[table_name], file_path, meta_attributes)
                archive.write(file_path, file_name)
        return Path(archive_name)

    def _await_import_job(self, job: str):
        while True:
            sleep(5)
            import_run = self.get_by_id(
                "sys_ImportRun", job, attributes="status,message"
            )
            if import_run["status"] == "FAILED":
                raise MolgenisRequestError(import_run["message"])
            if import_run["status"] != "RUNNING":
                return

    def _set_urls(self, url: str):
        """ Sets the root and API URLs.
        Historically, the URL had to be passed with '/api' at the end. This method is for backwards compatibility and
        allows for URLs both with and without the '/api' postfix.
        """
        self._root_url = url.rstrip('/').rstrip('/api') + '/'
        self._api_url = self._root_url + 'api/'
