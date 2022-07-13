import os
import unittest

import molgenis.client as molgenis


class ResponseMock:
    def __init__(self, content):
        self.content = content


class ExceptionMock:
    def __init__(self, message, response):
        self.args = [message]
        self.response = ResponseMock(response)


class TestStringMethods(unittest.TestCase):
    """
    Tests the client against a running MOLGENIS.
    """

    host = os.getenv('CI_HOST', 'http://localhost:8080')
    password = os.getenv('CI_PASSWORD', 'admin')
    api_url = host

    user_entity = 'sys_sec_User'
    ref_entity = 'org_molgenis_test_python_TypeTestRef'
    entity = 'org_molgenis_test_python_TypeTest'
    expected_ref_data = [
        {'_href': '/api/v2/org_molgenis_test_python_TypeTestRef/ref1', 'value': 'ref1', 'label': 'label1'},
        {'_href': '/api/v2/org_molgenis_test_python_TypeTestRef/ref2', 'value': 'ref2', 'label': 'label2'},
        {'_href': '/api/v2/org_molgenis_test_python_TypeTestRef/ref3', 'value': 'ref3', 'label': 'label3'},
        {'_href': '/api/v2/org_molgenis_test_python_TypeTestRef/ref4', 'value': 'ref4', 'label': 'label4'},
        {'_href': '/api/v2/org_molgenis_test_python_TypeTestRef/ref5', 'value': 'ref5', 'label': 'label5'},
    ]
    session = molgenis.Session(api_url)
    session.login('admin', password)

    def _try_delete(self, entity_type, entity_ids):
        # Try to remove because if a previous test failed, possibly the refs you're about to add are not removed yet
        try:
            self.session.delete_list(entity_type, entity_ids)
        except Exception as e:
            print(e)

    def _try_add(self, entity_type, entities):
        # Try to add because if a previous test failed, possibly the refs you're about to remove are not added yet
        try:
            self.session.add_all(entity_type, entities)
        except Exception as e:
            print(e)

    @classmethod
    def setUpClass(cls):
        cwd = os.getcwd()
        if cwd.endswith('tests'):
            os.chdir('..')
        response = cls.session.upload_zip('./tests/resources/all_datatypes.zip').split('/')
        run_entity_type = response[-2]
        run_id = response[-1]
        status_info = cls.session.get_by_id(run_entity_type, run_id)
        while status_info['status'] == 'RUNNING':
            status_info = cls.session.get_by_id(run_entity_type, run_id)
        if status_info['status'] == 'FAILED':
            raise Exception(f"Importing test data failed: {status_info['message']}", )

    @classmethod
    def tearDownClass(cls):
        cls.session.delete('sys_md_Package', 'org')
        cls.session.logout()

    def test_login_logout_and_get_MolgenisUser(self):
        s = molgenis.Session(self.api_url)
        s.login('admin', self.password)
        s.get(self.user_entity)
        s.logout()
        try:
            s._get_batch(self.user_entity)
        except Exception as e:
            response = e.args[1]
            response.connection.close()
            assert response.status_code == 401

    def test_token_session_and_get_MolgenisUser(self):
        token = 'token_session_test'
        admin = self.session.get('sys_sec_User', q='username==admin', attributes='id')
        self.session.add('sys_sec_Token', data={'User': admin[0]['id'],
                                                'token': token,
                                                'creationDate': '2000-01-01T01:01:01'})

        token_session = molgenis.Session(self.api_url, token=token)
        token_session.get(self.user_entity)
        token_session.logout()

        try:
            token_session._get_batch(self.user_entity)
        except Exception as e:
            response = e.args[1]
            response.connection.close()
            assert response.status_code == 401

    def test_no_login_and_get_MolgenisUser(self):
        s = molgenis.Session(self.api_url)
        try:
            s._get_batch(self.user_entity)
        except Exception as e:
            response = e.args[1]
            response.connection.close()
            assert response.status_code == 401

    def test_session_url_with_api_slash(self):
        url = 'root/api/'

        session = molgenis.Session(url)

        assert session._root_url == 'root/'
        assert session._api_url == 'root/api/'

    def test_session_url_with_api(self):
        url = 'root/api'

        session = molgenis.Session(url)

        assert session._root_url == 'root/'
        assert session._api_url == 'root/api/'

    def test_session_url_slash(self):
        url = 'root/'

        session = molgenis.Session(url)

        assert session._root_url == 'root/'
        assert session._api_url == 'root/api/'

    def test_session_url(self):
        url = 'root'

        session = molgenis.Session(url)

        assert session._root_url == 'root/'
        assert session._api_url == 'root/api/'

    def test_upload_zip(self):
        self._try_delete('sys_md_EntityType', ['org_molgenis_test_python_sightings'])
        response = self.session.upload_zip('./tests/resources/sightings_test.zip').split('/')
        run_entity_type = response[-2]
        run_id = response[-1]
        status_info = self.session.get_by_id(run_entity_type, run_id)
        while status_info['status'] == 'RUNNING':
            status_info = self.session.get_by_id(run_entity_type, run_id)
        self.assertEqual('FINISHED', status_info['status'])

    def test_upload_zip_await(self):
        self._try_delete('sys_md_EntityType', ['org_molgenis_test_python_sightings'])
        response = self.session.upload_zip('./tests/resources/sightings_test.zip', asynchronous=False).split('/')
        run_entity_type = response[-2]
        run_id = response[-1]
        status_info = self.session.get_by_id(run_entity_type, run_id)
        self.assertEqual('FINISHED', status_info['status'])

    def test_upload_zip_param(self):
        self._try_delete('sys_md_EntityType', ['org_molgenis_test_python_sightings'])
        response = self.session.upload_zip('./tests/resources/sightings_test.zip', asynchronous=False,
                                           data_action=molgenis.ImportDataAction.ADD_UPDATE_EXISTING, metadata_action=molgenis.ImportMetadataAction.ADD).split('/')
        run_entity_type = response[-2]
        run_id = response[-1]
        status_info = self.session.get_by_id(run_entity_type, run_id)
        self.assertEqual('FINISHED', status_info['status'])

    def test_import_data(self):
        self._try_delete(self.ref_entity, ['ref55'])
        self.assertEqual('ref55', self.session.add(self.ref_entity, {"value": "ref55", "label": "label55"}))
        try:
            self.session.get_by_id(self.ref_entity, "ref66", "label")
        except Exception as e:
            message = e.args[0]
            expected = "404 Client Error:  for url: {}/api/v2/org_molgenis_test_python_TypeTestRef/ref66?attrs=label: Unknown entity with 'value' 'ref66' of type 'TypeTestRef'.".format(self.api_url)
            self.assertEqual(expected, message)
        data={}
        data[self.ref_entity] = [{"value": "ref55", "label": "updated-label55"}, {"value": "ref66", "label": "label66"}]
        try:
            self.session.import_data(data, molgenis.ImportDataAction.ADD_UPDATE_EXISTING, molgenis.ImportMetadataAction.IGNORE)
        except Exception as e:
            raise Exception(e)
        item55 = self.session.get_by_id(self.ref_entity, "ref55", "label")
        self.assertEqual("updated-label55", item55["label"])
        item66 = self.session.get_by_id(self.ref_entity, "ref66", "label")
        self.assertEqual("label66", item66["label"])
        self.session.delete_list(self.ref_entity, ['ref55', "ref66"])

    def test_delete_row(self):
        self._try_add(self.ref_entity, [{"value": "ref55", "label": "label55"}])
        response = self.session.delete(self.ref_entity, 'ref55')
        self.assertEqual(str(response), '<Response [204]>', 'Check status code')
        items = self.session.get(self.ref_entity)
        self.assertEqual(len(items), 5, 'Check if items that were not deleted are still present')
        no_items = self.session.get(self.ref_entity, q='value=in=(ref55)')
        self.assertEqual(len(no_items), 0, 'Check if item that was deleted is really deleted')

    def test_delete_data(self):
        self._try_delete('sys_md_EntityType', ['org_molgenis_test_python_sightings'])
        response = self.session.upload_zip('./tests/resources/sightings_test.zip').split('/')
        run_entity_type = response[-2]
        run_id = response[-1]
        status_info = self.session.get_by_id(run_entity_type, run_id)
        while status_info['status'] == 'RUNNING':
            status_info = self.session.get_by_id(run_entity_type, run_id)
        self.session.delete('org_molgenis_test_python_sightings')
        number_of_rows = self.session.get('org_molgenis_test_python_sightings', raw=True)['total']
        self.assertEqual(0, number_of_rows)

    def test_add_all(self):
        self._try_delete(self.ref_entity, ['ref55', 'ref57'])
        response = self.session.add_all(self.ref_entity,
                                        [{"value": "ref55", "label": "label55"},
                                         {"value": "ref57", "label": "label57"}])
        self.assertEqual(['ref55', 'ref57'], response)
        item55 = self.session.get(self.ref_entity, q="value==ref55")[0]
        self.assertEqual({"value": "ref55", "label": "label55", "_href": "/api/v2/" + self.ref_entity + "/ref55"},
                         item55)
        self.session.delete(self.ref_entity, 'ref55')
        self.session.delete(self.ref_entity, 'ref57')

    def test_add_all_error(self):
        try:
            self.session.add_all(self.ref_entity, [{"value": "ref55"}])
        except Exception as e:
            message = e.args[0]
            expected = "400 Client Error:  for url: {}/api/v2/org_molgenis_test_python_TypeTest" \
                       "Ref: The attribute 'label' of entity 'org_molgenis_test_python_TypeTestRef' can not be null.".format(
                self.api_url)
            self.assertEqual(expected, message)

    def test_delete_list(self):
        self._try_add(self.ref_entity, [{"value": "ref55", "label": "label55"},
                                        {"value": "ref57", "label": "label57"}])
        response = self.session.delete_list(self.ref_entity, ['ref55', 'ref57'])
        self.assertEqual(str(response), '<Response [204]>', 'Check status code')
        items = self.session.get(self.ref_entity)
        self.assertEqual(len(items), 5, 'Check if items that were not deleted are still present')
        no_items = self.session.get(self.ref_entity, q='value=in=(ref55,ref57)')
        self.assertEqual(len(no_items), 0, 'Check if items that were deleted are really deleted')

    def test_add_dict(self):
        self._try_delete(self.ref_entity, ['ref55'])
        self.assertEqual('ref55', self.session.add(self.ref_entity, {"value": "ref55", "label": "label55"}))
        self.session.delete(self.ref_entity, 'ref55')

    def test_update_one(self):
        self._try_delete(self.ref_entity, ['ref55'])
        self.assertEqual('ref55', self.session.add(self.ref_entity, {"value": "ref55", "label": "label55"}))
        try:
            self.session.update_one(self.ref_entity, 'ref55', 'label', 'updated-label55')
        except Exception as e:
            raise Exception(e)
        item55 = self.session.get_by_id(self.ref_entity, "ref55", "label")
        self.assertEqual("updated-label55", item55["label"])
        self.session.delete(self.ref_entity, 'ref55')

    def test_update_one_error(self):
        try:
            self.session.update_one(self.ref_entity, 'ref555', 'label', 'updated-label555')
        except Exception as e:
            message = e.args[0]
            expected = "404 Client Error:  for url: {}/api/v1/org_molgenis_test_python_TypeTestRef" \
                       "/ref555/label: Unknown entity with 'value' 'ref555' of type 'TypeTestRef'.".format(self.api_url)
            self.assertEqual(expected, message)

    def test_update_all(self):
        self._try_delete(self.ref_entity, ['ref55', 'ref66'])
        self.assertEqual(['ref55', "ref66"], self.session.add_all(self.ref_entity, [{"value": "ref55", "label": "label55"}, {"value": "ref66", "label": "label66"}]))
        try:
            self.session.update_all(self.ref_entity, [{"value": "ref55", "label": "updated-label55"}, {"value": "ref66", "label": "updated-label66"}])
        except Exception as e:
            raise Exception(e)
        item55 = self.session.get_by_id(self.ref_entity, "ref55", "label")
        self.assertEqual("updated-label55", item55["label"])
        item66 = self.session.get_by_id(self.ref_entity, "ref66", "label")
        self.assertEqual("updated-label66", item66["label"])
        self.session.delete_list(self.ref_entity, ['ref55', "ref66"])

    def test_update_all_error(self):
        try:
            self.session.update_all(self.ref_entity, [{"value": "ref555", "label": "updated-label555"}, {"value": "ref666", "label": "updated-label666"}])
        except Exception as e:
            message = e.args[0]
            expected = "400 Client Error:  for url: {}/api/v2/org_molgenis_test_python_TypeTestRef: Cannot update [org_molgenis_test_python_TypeTestRef] with id [ref555] because it does not exist".format(self.api_url)
            self.assertEqual(expected, message)

    def test_upsert(self):
        self._try_delete(self.ref_entity, ['ref55'])
        self.assertEqual('ref55', self.session.add(self.ref_entity, {"value": "ref55", "label": "label55"}))
        try:
            self.session.get_by_id(self.ref_entity, "ref66", "label")
        except Exception as e:
            message = e.args[0]
            expected = "404 Client Error:  for url: {}/api/v2/org_molgenis_test_python_TypeTestRef/ref66?attrs=label: Unknown entity with 'value' 'ref66' of type 'TypeTestRef'.".format(self.api_url)
            self.assertEqual(expected, message)
        try:
            self.session.upsert(self.ref_entity, [{"value": "ref55", "label": "updated-label55"}, {"value": "ref66", "label": "label66"}])
        except Exception as e:
            raise Exception(e)
        item55 = self.session.get_by_id(self.ref_entity, "ref55", "label")
        self.assertEqual("updated-label55", item55["label"])
        item66 = self.session.get_by_id(self.ref_entity, "ref66", "label")
        self.assertEqual("label66", item66["label"])
        self.session.delete_list(self.ref_entity, ['ref55', "ref66"])

    def test_add_kwargs(self):
        self._try_delete(self.ref_entity, ['ref55'])
        self.assertEqual('ref55', self.session.add(self.ref_entity, value="ref55", label="label55"))
        item55 = self.session.get(self.ref_entity, q="value==ref55")[0]
        self.assertEqual({"value": "ref55", "label": "label55", "_href": "/api/v2/" + self.ref_entity + "/ref55"},
                         item55)
        self.session.delete(self.ref_entity, 'ref55')

    def test_add_merge_dict_kwargs(self):
        self._try_delete(self.ref_entity, ['ref55'])
        self.assertEqual('ref55', self.session.add(self.ref_entity, {"value": "ref55"}, label="label55"))
        item55 = self.session.get(self.ref_entity, q="value==ref55")[0]
        self.assertEqual({"value": "ref55", "label": "label55", "_href": "/api/v2/" + self.ref_entity + "/ref55"},
                         item55)
        self.session.delete(self.ref_entity, 'ref55')

    def test_get(self):
        data = self.session.get(self.ref_entity)
        self.assertEqual(self.expected_ref_data, data)

    def test_get_raw(self):
        data = self.session.get(self.ref_entity, raw=True)
        self.assertTrue('meta' in data)
        self.assertTrue('items' in data)

    def test_get_query(self):
        data = self.session.get(self.ref_entity, q='value==ref1')
        expected = [{'_href': '/api/v2/org_molgenis_test_python_TypeTestRef/ref1', 'value': 'ref1', 'label': 'label1'}]
        self.assertEqual(expected, data)

    def test_get_num(self):
        data = self.session.get(self.ref_entity, num=2)
        self.assertEqual(2, len(data))

    def test_get_batch(self):
        data = self.session.get(self.ref_entity, batch_size=2)
        self.assertEqual(self.expected_ref_data, data)

    def test_get_expand(self):
        data = self.session.get(self.entity, expand='xcomputedxref')
        first_item = data[0]
        expected = {"_href": "/api/v2/org_molgenis_test_python_Location/5", "Chromosome": "str1", "Position": 5}
        self.assertEqual(47, len(first_item))
        self.assertEqual(expected, first_item['xcomputedxref'])

    def test_get_expand_attrs(self):
        data = self.session.get(self.entity, expand='xcomputedxref',
                                attributes='id,xcomputedxref')
        first_item = data[0]
        expected = {"_href": "/api/v2/org_molgenis_test_python_Location/5", "Chromosome": "str1", "Position": 5}
        self.assertEqual(len(first_item), 3)
        self.assertEqual(expected, first_item['xcomputedxref'])

    def test_get_uploadable(self):
        data = self.session.get(self.entity, uploadable=True)
        first_item = data[0]['xcategorical_value']
        expected = 'ref1'
        self.assertEqual(expected, first_item)

    def test_get_entity_meta(self):
        meta = self.session.get_entity_meta_data(self.user_entity)
        self.assertEqual('username', meta['labelAttribute'])

    def test_get_attribute_meta(self):
        meta = self.session.get_attribute_meta_data(self.user_entity, 'username')
        self.assertEqual({'href': '/api/v1/sys_sec_User/meta/username', 'fieldType': 'STRING', 'name': 'username',
                          'label': 'Username', 'attributes': [], 'enumOptions': [], 'maxLength': 255, 'auto': False,
                          'nillable': False, 'readOnly': True, 'labelAttribute': True, 'unique': True, 'visible': True,
                          'lookupAttribute': True, 'isAggregatable': False,
                          'validationExpression': "regex('^\\\\S.+\\\\S$', {username})"},
                         meta)

    def test_get_meta(self):
        meta = self.session.get_meta(self.user_entity)
        self.assertEqual('Username', meta["attributes"]["items"][1]["data"]["label"])

    def test_get_meta_expand(self):
        meta = self.session.get_meta(self.entity, expand=True)
        self.assertEqual("value", meta["attributes"]["items"][6]["data"]["refEntityType"]["attributes"]["items"][0]["data"]["name"])

    def test_get_meta_abstract(self):
        meta = self.session.get_meta("sys_mail_JavaMailProperty", abstract=True)
        attr=[]
        expected = ["MailSettings", "key", "value"]
        for item in meta["attributes"]["items"]:
            attr.append(item["data"]["label"])
        self.assertEqual(expected, attr)

    def test_get_by_id(self):
        data = self.session.get_by_id(self.ref_entity, 'ref1')
        del data['_meta']
        self.assertEqual(self.expected_ref_data[0], data)

    def test_get_by_id_uploadable(self):
        data = self.session.get_by_id(self.ref_entity, 'ref1', uploadable=True)
        self.assertEqual({'label': 'label1', 'value': 'ref1'}, data)

    def test_get_by_id_expand(self):
        data = self.session.get_by_id(self.entity, '1', expand='xcomputedxref')
        expected = {"_href": "/api/v2/org_molgenis_test_python_Location/5", "Chromosome": "str1", "Position": 5}
        self.assertEqual(expected, data['xcomputedxref'])

    def test_get_by_id_no_expand(self):
        data = self.session.get_by_id(self.entity, '1')
        expected = {"_href": "/api/v2/org_molgenis_test_python_Location/5", "Position": 5}
        self.assertEqual(expected, data['xcomputedxref'])

    def test_build_api_url_complex(self):
        base_url = 'https://test.frl/api/test'
        possible_options = {'q': 'x==1',
                            'attrs': ['x,y', 'y'],
                            'num': 1000,
                            'start': 1000,
                            'sort': ['x', 'desc']}
        generated_url = self.session._build_api_url(base_url, possible_options)
        # Only check the contents of the operators because their order is random
        expected_sort = 'x:desc'
        expected_attrs = ['x', 'y(*)']
        expected_num = '1000'
        expected_start = '1000'
        expected_q = 'q=x==1'
        operators = generated_url.split('?')[1].split('&')
        observed_q = operators[0]
        observed_attrs = operators[1].replace('attrs=', '').split(',')
        observed_num = operators[2].replace('num=', '')
        observed_start = operators[3].replace('start=', '')
        observed_sort = operators[4].replace('sort=', '')
        self.assertEqual(expected_q, observed_q)
        self.assertEqual(expected_num, observed_num)
        self.assertEqual(expected_start, observed_start)
        self.assertEqual(expected_sort, observed_sort)
        self.assertEqual(sorted(expected_attrs), sorted(observed_attrs))

    def test_build_api_url_simple(self):
        base_url = 'https://test.frl/api/test'
        possible_options = {'q': None,
                            'attrs': [None, None],
                            'num': 100,
                            'start': 0,
                            'sort': [None, None]}
        generated_url = self.session._build_api_url(base_url, possible_options)
        expected = 'https://test.frl/api/test'
        self.assertEqual(expected, generated_url)

    def test_build_api_url_less_complex(self):
        base_url = 'https://test.frl/api/test'
        possible_options = {'q': None,
                            'attrs': [None, 'y'],
                            'num': 100,
                            'start ': 0,
                            'sort': ['x', None]}
        generated_url = self.session._build_api_url(base_url, possible_options)
        # Only check the contents of the operators because their order is random
        expected_sort = 'x'
        expected_attrs = ['*', 'y(*)']
        operators = generated_url.split('?')[1].split('&')
        observed_attrs = operators[0].replace('attrs=', '').split(',')
        observed_sort = operators[1].replace('sort=', '')
        self.assertEqual(expected_sort, observed_sort)
        self.assertEqual(sorted(expected_attrs), sorted(observed_attrs))

    def test_build_api_url_error(self):
        base_url = 'https://test.frl/api/test'
        possible_options = {'q': [{"field": "x", "operator": "EQUALS", "value": "1"}],
                            'attrs': [None, 'y'],
                            'num': 100,
                            'start': 0,
                            'sort': ['x', None]}
        with self.assertRaises(TypeError):
            self.session._build_api_url(base_url, possible_options)

    def test_raise_exception_with_missing_content(self):
        msg = 'message'
        ex = ExceptionMock(msg, None)
        try:
            self.session._raise_exception(ex)
        except Exception as e:
            message = e.args[0]
            expected = msg
            self.assertEqual(expected, message)


if __name__ == '__main__':
    unittest.main()
