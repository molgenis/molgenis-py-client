import json
import os
import requests

try:
    from urllib.parse import quote_plus
except ImportError:
    # Python 2
    # noinspection PyUnresolvedReferences
    from urllib import quote_plus


class MolgenisRequestError(Exception):
    def __init__(self, error, response=False):
        self.message = error
        if response:
            self.response = response


class Session:
    """Representation of a session with the MOLGENIS REST API.
    Usage:

    >>> session = Session('http://localhost:8080/api/')
    >>> session.login('user', 'password')
    >>> session.get('Person')
    """

    def __init__(self, url="http://localhost:8080/api/"):
        """Constructs a new Session.
        Args:
        url -- URL of the REST API. Should be of form 'http[s]://<molgenis server>[:port]/api/'

        Examples:
        >>> session = Session('http://localhost:8080/api/')
        """
        self._url = url
        self._session = requests.Session()
        self._token = None

    def login(self, username, password):
        """Logs in a user and stores the acquired session token in this Session object.

        Args:
        username -- username for a registered molgenis user
        password -- password for the user
        """
        self._session.cookies.clear()
        response = self._session.post(self._url + "v1/login",
                                      data=json.dumps({"username": username, "password": password}),
                                      headers={"Content-Type": "application/json"})
        try:
            response.raise_for_status()
        except requests.RequestException as ex:
            self._raise_exception(ex)

        self._token = response.json()['token']

    def logout(self):
        """Logs out the current session token."""
        response = self._session.post(self._url + "v1/logout",
                                      headers=self._get_token_header())
        try:
            response.raise_for_status()
        except requests.RequestException as ex:
            self._raise_exception(ex)

        self._token = None
        self._session.cookies.clear()

    def get_by_id(self, entity, id_, attributes=None, expand=None):
        """Retrieves a single entity row from an entity repository.

        Args:
        entity -- fully qualified name of the entity
        id -- the value for the idAttribute of the entity
        attributes -- The list of attributes to retrieve
        expand -- the attributes to expand

        Examples:
        session.get('Person', 'John')
        """
        response = self._session.get(self._url + "v2/" + quote_plus(entity) + '/' + quote_plus(id_),
                                     headers=self._get_token_header(),
                                     params={"attributes": attributes, "expand": expand})
        try:
            response.raise_for_status()
        except requests.RequestException as ex:
            self._raise_exception(ex)

        result = response.json()
        response.close()
        return result

    def get(self, entity, q=None, attributes=None, num=100, start=0, sort_column=None, sort_order=None, raw=False,
            expand=None):
        """Retrieves entity rows from an entity repository.

        Args:
        entity -- fully qualified name of the entity
        q -- query in rsql format, see our RSQL documentation for details
            (https://molgenis.gitbooks.io/molgenis/content/developer_documentation/ref-RSQL.html)
        attributes -- The list of attributes to retrieve
        expand -- the attributes to expand
        num -- the amount of entity rows to retrieve (maximum is 10,000)
        start -- the index of the first row to retrieve (zero indexed)
        sortColumn -- the attribute to sort on
        sortOrder -- the order to sort in
        raw -- when true, the complete REST response will be returned, rather than the data items alone

        Examples:
        >>> session = Session('http://localhost:8080/api/')
        >>> session.get('Person')
        >>> session.get(entity='Person', q='name=="Henk"', attributes=['name', 'age'])
        >>> session.get(entity='Person', sort_column='age', sort_order='desc')
        >>> session.get('Person', raw=True)
        """
        possible_options = {'q': q,
                            'attrs': [attributes, expand],
                            'num': num,
                            'start': start,
                            'sort': [sort_column, sort_order]}

        url = self._build_api_url(self._url + "v2/" + quote_plus(entity), possible_options)
        response = self._session.get(url, headers=self._get_token_header())

        try:
            response.raise_for_status()
        except requests.RequestException as ex:
            self._raise_exception(ex)

        if not raw:
            return response.json()["items"]
        else:
            return response.json()

    def add(self, entity, data=None, files=None, **kwargs):
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

        response = self._session.post(self._url + "v1/" + quote_plus(entity),
                                      headers=self._get_token_header(),
                                      data=self._merge_two_dicts(data, kwargs),
                                      files=files)
        try:
            response.raise_for_status()
        except requests.RequestException as ex:
            self._raise_exception(ex)

        return response.headers["Location"].split("/")[-1]

    def add_all(self, entity, entities):
        """Adds multiple entity rows to an entity repository."""
        response = self._session.post(self._url + "v2/" + quote_plus(entity),
                                      headers=self._get_token_header_with_content_type(),
                                      data=json.dumps({"entities": entities}))

        try:
            response.raise_for_status()
        except requests.RequestException as ex:
            self._raise_exception(ex)

        return [resource["href"].split("/")[-1] for resource in response.json()["resources"]]

    def update_one(self, entity, id_, attr, value):
        """Updates one attribute of a given entity in a table with a given value"""
        response = self._session.put(self._url + "v1/" + quote_plus(entity) + "/" + id_ + "/" + attr,
                                     headers=self._get_token_header_with_content_type(),
                                     data=json.dumps(value))

        try:
            response.raise_for_status()
        except requests.RequestException as ex:
            self._raise_exception(ex)

        return response

    def delete(self, entity, id_=None):
        """Deletes a single entity row or all rows (if id_ not specified) from an entity repository."""
        url = self._url + "v1/" + quote_plus(entity)
        if id_:
            url = url + "/" + quote_plus(id_)

        response = self._session.delete(url, headers=self._get_token_header())
        try:
            response.raise_for_status()
        except requests.RequestException as ex:
            self._raise_exception(ex)

        return response

    def delete_list(self, entity, entities):
        """Deletes multiple entity rows to an entity repository, given a list of id's."""
        response = self._session.delete(self._url + "v2/" + quote_plus(entity),
                                        headers=self._get_token_header_with_content_type(),
                                        data=json.dumps({"entityIds": entities}))
        try:
            response.raise_for_status()
        except requests.RequestException as ex:
            self._raise_exception(ex)

        return response

    def get_entity_meta_data(self, entity):
        """Retrieves the metadata for an entity repository."""
        response = self._session.get(self._url + "v1/" + quote_plus(entity) + "/meta?expand=attributes",
                                     headers=self._get_token_header())
        try:
            response.raise_for_status()
        except requests.RequestException as ex:
            self._raise_exception(ex)

        return response.json()

    def get_attribute_meta_data(self, entity, attribute):
        """Retrieves the metadata for a single attribute of an entity repository."""
        response = self._session.get(self._url + "v1/" + quote_plus(entity) + "/meta/" + quote_plus(attribute),
                                     headers=self._get_token_header())
        try:
            response.raise_for_status()
        except requests.RequestException as ex:
            self._raise_exception(ex)

        return response.json()

    def upload_zip(self, meta_data_zip):
        """Uploads a given zip with data and metadata"""
        header = self._get_token_header()
        zip_file = open(os.path.abspath(meta_data_zip), 'rb')
        files = {'file': zip_file}
        url = self._url.strip('/api/') + '/plugin/importwizard/importFile'
        response = requests.post(url, headers=header, files=files)
        zip_file.close()
        try:
            response.raise_for_status()
        except requests.RequestException as ex:
            self._raise_exception(ex)

        return response.content.decode("utf-8")

    def _get_token_header(self):
        """Creates an 'x-molgenis-token' header for the current session."""
        try:
            return {"x-molgenis-token": self._token}
        except AttributeError:
            return {}

    def _get_token_header_with_content_type(self):
        """Creates an 'x-molgenis-token' header for the current session and a 'Content-Type: application/json' header"""
        headers = self._get_token_header()
        headers.update({"Content-Type": "application/json"})
        return headers

    @staticmethod
    def _process_query(option_value, option):
        """Add query to operators and raise exception when query value is invalid"""
        if type(option_value) == list:
            raise TypeError('Please specify your query in the RSQL format.')
        else:
            return '{}={}'.format(option, option_value)

    @staticmethod
    def _process_sort(option_value):
        """Converts the sort and sort order to a sort attribute compatible with the REST API v2"""
        if option_value[0] and not option_value[1]:
            return 'sort=' + option_value[0]
        elif option_value[0] and option_value[1]:
            return 'sort={}:{}'.format(option_value[0], option_value[1])

    @staticmethod
    def _split_if_not_none(operator):
        """Returns empty list if operator is None, else splits the operator string by comma to return a list"""
        return operator.split(',') if operator else []

    @staticmethod
    def _update_operators_if_operator_exists(operators, operator):
        """Append operator to operators if the operator exists. Returns the new operators"""
        if operator:
            operators.append(operator)
        return operators

    def _merge_attrs(self, attr_expands):
        """Converts the attrs and expands to an attr attribute compatible with the REST API v2"""
        # Make a list of attrs and expands
        attrs = self._split_if_not_none(attr_expands[0])
        expands = self._split_if_not_none(attr_expands[1])
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

    def _build_api_url(self, base_url, possible_options):
        """This function builds the api url for the get request, converting the api v1 compliant operators to v2
        operators to enable backwards compatibility of the python api when switching to api v2"""
        operators = []
        for option in possible_options:
            option_value = possible_options[option]
            if option == 'q' and option_value:
                q = self._process_query(option_value, option)
                operators = self._update_operators_if_operator_exists(operators, q)
            elif option == 'sort':
                sort = self._process_sort(option_value)
                operators = self._update_operators_if_operator_exists(operators, sort)
            elif option == 'attrs':
                attrs = self._merge_attrs(option_value)
                operators = self._update_operators_if_operator_exists(operators, attrs)
            elif option_value and not (option == 'num' and option_value == 100):
                operators.append('{}={}'.format(option, option_value))
        url = '{}?{}'.format(base_url, '&'.join(operators))

        if url == base_url + '?':
            return base_url
        else:
            return url

    @staticmethod
    def _merge_two_dicts(x, y):
        """Given two dicts, merge them into a new dict as a shallow copy."""
        z = x.copy()
        z.update(y)
        return z

    @staticmethod
    def _raise_exception(ex):
        """Raises an exception with error message from molgenis"""
        message = ex.args[0]
        if ex.response.content:
            error = json.loads(ex.response.content.decode("utf-8"))['errors'][0]['message']
            error_msg = '{}: {}'.format(message, error)
            raise MolgenisRequestError(error_msg, ex.response)
        else:
            raise MolgenisRequestError('{}'.format(message))
