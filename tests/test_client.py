import unittest

import requests

import molgenis.client as molgenis


class TestStringMethods(unittest.TestCase):
    """
    Tests the client against a running MOLGENIS.
    """

    api_url = "http://localhost:8080/api/"

    no_readmeta_permission_user_msg = "No 'Read metadata' permission on entity type 'User' with id 'sys_sec_User'."
    user_entity = 'sys_sec_User'
    ref_entity = 'org_molgenis_test_python_TypeTestRef'
    session = molgenis.Session(api_url)
    session.login('admin', 'admin')

    @classmethod
    def setUpClass(cls):
        response = cls.session.upload_zip('./resources/all_datatypes.zip').split('/')
        run_entity_type = response[-2]
        run_id = response[-1]
        status_info = cls.session.get_by_id(run_entity_type, run_id)
        while status_info['status'] == 'RUNNING':
            status_info = cls.session.get_by_id(run_entity_type, run_id)

    @classmethod
    def tearDownClass(cls):
        cls.session.delete('sys_md_Package', 'org')
        cls.session.logout()

    def test_login_logout_and_get_MolgenisUser(self):
        s = molgenis.Session(self.api_url)
        response = s.login('admin', 'admin')
        s.get(self.user_entity)
        s.logout()
        try:
            s.get(self.user_entity)
        except requests.exceptions.HTTPError as e:
            self.assertEqual(e.response.status_code, 401)
            self.assertEqual(e.response.json()['errors'][0]['message'], self.no_readmeta_permission_user_msg)

    def test_no_login_and_get_MolgenisUser(self):
        s = molgenis.Session(self.api_url)
        try:
            s.get(self.user_entity)
        except requests.exceptions.HTTPError as e:
            self.assertEqual(e.response.status_code, 401)
            self.assertEqual(e.response.json()['errors'][0]['message'], self.no_readmeta_permission_user_msg)

    def test_upload_zip(self):
        response = self.session.upload_zip('./resources/sightings_test.zip').split('/')
        run_entity_type = response[-2]
        run_id = response[-1]
        status_info = self.session.get_by_id(run_entity_type, run_id)
        while status_info['status'] == 'RUNNING':
            status_info = self.session.get_by_id(run_entity_type, run_id)
        self.assertEqual(status_info['status'], 'FINISHED')

    def test_add_all(self):
        try:
            self.session.delete(self.ref_entity, 'ref55')
            self.session.delete(self.ref_entity, 'ref57')
        except Exception as e:
            raise Exception(e)
        response = self.session.add_all(self.ref_entity,
                                        [{"value": "ref55", "label": "label55"},
                                         {"value": "ref57", "label": "label57"}])
        self.assertEqual(['ref55', 'ref57'], response)
        item55 = self.session.get(self.ref_entity, q="value==ref55")[0]
        self.assertEqual({"value": "ref55", "label": "label55", "_href": "/api/v2/" + self.ref_entity + "/ref55"},
                         item55)
        self.session.delete(self.ref_entity, 'ref55')
        self.session.delete(self.ref_entity, 'ref57')

    def test_add_dict(self):
        try:
            self.session.delete(self.ref_entity, 'ref55')
        except Exception as e:
            raise Exception(e)
        self.assertEqual('ref55', self.session.add(self.ref_entity, {"value": "ref55", "label": "label55"}))
        self.session.delete(self.ref_entity, 'ref55')

    def test_update_one(self):
        try:
            self.session.delete(self.ref_entity, 'ref55')
        except Exception as e:
            raise Exception(e)
        self.assertEqual('ref55', self.session.add(self.ref_entity, {"value": "ref55", "label": "label55"}))
        try:
            self.session.update_one(self.ref_entity, 'ref55', 'label', 'updated-label55');
        except Exception as e:
            raise Exception(e)
        item55 = self.session.get_by_id(self.ref_entity, "ref55", ["label"])
        self.assertEqual("updated-label55", item55["label"])
        self.session.delete(self.ref_entity, 'ref55')

    def test_add_kwargs(self):
        try:
            self.session.delete(self.ref_entity, 'ref55')
        except Exception as e:
            raise Exception(e)
        self.assertEqual('ref55', self.session.add(self.ref_entity, value="ref55", label="label55"))
        item55 = self.session.get(self.ref_entity, q="value==ref55")[0]
        self.assertEqual({"value": "ref55", "label": "label55", "_href": "/api/v2/" + self.ref_entity + "/ref55"},
                         item55)
        self.session.delete(self.ref_entity, 'ref55')

    def test_add_merge_dict_kwargs(self):
        try:
            self.session.delete(self.ref_entity, 'ref55')
        except Exception as e:
            raise Exception(e)
        self.assertEqual('ref55', self.session.add(self.ref_entity, {"value": "ref55"}, label="label55"))
        item55 = self.session.get(self.ref_entity, q="value==ref55")[0]
        self.assertEqual({"value": "ref55", "label": "label55", "_href": "/api/v2/" + self.ref_entity + "/ref55"},
                         item55)
        self.session.delete(self.ref_entity, 'ref55')

    def test_get(self):
        data = self.session.get(self.ref_entity)
        expected = [{'_href': '/api/v2/org_molgenis_test_python_TypeTestRef/ref1', 'value': 'ref1', 'label': 'label1'},
                    {'_href': '/api/v2/org_molgenis_test_python_TypeTestRef/ref2', 'value': 'ref2', 'label': 'label2'},
                    {'_href': '/api/v2/org_molgenis_test_python_TypeTestRef/ref3', 'value': 'ref3', 'label': 'label3'},
                    {'_href': '/api/v2/org_molgenis_test_python_TypeTestRef/ref4', 'value': 'ref4', 'label': 'label4'},
                    {'_href': '/api/v2/org_molgenis_test_python_TypeTestRef/ref5', 'value': 'ref5', 'label': 'label5'}]
        self.assertEqual(data, expected)

    def test_get_query(self):
        data = self.session.get(self.ref_entity, q='value==ref1')
        expected = [{'_href': '/api/v2/org_molgenis_test_python_TypeTestRef/ref1', 'value': 'ref1', 'label': 'label1'}]
        self.assertEqual(data, expected)

    def test_get_num(self):
        data = self.session.get(self.ref_entity, num=2)
        self.assertEqual(len(data), 2)

    def test_get_expand(self):
        data = self.session.get(self.ref_entity.replace('Ref', ''), expand='xcomputedxref')
        first_item = data[0]
        expected = {"_href": "/api/v2/org_molgenis_test_python_Location/5", "Chromosome": "str1", "Position": 5}
        self.assertEqual(len(first_item), 47)
        self.assertEqual(first_item['xcomputedxref'], expected)

    def test_get_expand_attrs(self):
        data = self.session.get(self.ref_entity.replace('Ref', ''), expand='xcomputedxref',
                                attributes='id,xcomputedxref')
        first_item = data[0]
        expected = {"_href": "/api/v2/org_molgenis_test_python_Location/5", "Chromosome": "str1", "Position": 5}
        self.assertEqual(len(first_item), 3)
        self.assertEqual(first_item['xcomputedxref'], expected)

    def test_get_meta(self):
        meta = self.session.get_entity_meta_data(self.user_entity)
        self.assertEqual('username', meta['labelAttribute'])

    def test_get_attribute_meta(self):
        meta = self.session.get_attribute_meta_data(self.user_entity, 'username')
        self.assertEqual({'href': '/api/v1/sys_sec_User/meta/username', 'fieldType': 'STRING', 'name': 'username',
                          'label': 'Username', 'attributes': [], 'enumOptions': [], 'maxLength': 255, 'auto': False,
                          'nillable': False, 'readOnly': True, 'labelAttribute': True, 'unique': True, 'visible': True,
                          'lookupAttribute': True, 'isAggregatable': False,
                          'validationExpression': "$('username').matches(/^[\\S].+[\\S]$/).value()"},
                         meta)

    def test_build_api_url_complex(self):
        base_url = 'https://test.frl/api/test'
        possible_options = {'q': 'x==1',
                            'attrs': ['x,y', 'y'],
                            'num': 1000,
                            'start': 1000,
                            'sort': ['x', 'desc']}
        generated_url = self.session._build_api_url(base_url, possible_options)
        expected = 'https://test.frl/api/test?q=x==1&attrs=x,y(*)&num=1000&start=1000&sort=x:desc'
        self.assertEqual(generated_url, expected)

    def test_build_api_url_simple(self):
        base_url = 'https://test.frl/api/test'
        possible_options = {'q': None,
                            'attrs': [None, None],
                            'num': 100,
                            'start': 0,
                            'sort': [None, None]}
        generated_url = self.session._build_api_url(base_url, possible_options)
        expected = 'https://test.frl/api/test'
        self.assertEqual(generated_url, expected)

    def test_build_api_url_less_complex(self):
        base_url = 'https://test.frl/api/test'
        possible_options = {'q': None,
                            'attrs': [None, 'y'],
                            'num': 100,
                            'start': 0,
                            'sort': ['x', None]}
        generated_url = self.session._build_api_url(base_url, possible_options)
        expected = 'https://test.frl/api/test?attrs=y(*),*&sort=x'
        self.assertEqual(generated_url, expected)

    def test_build_api_url_error(self):
        base_url = 'https://test.frl/api/test'
        possible_options = {'q': [{"field": "x", "operator": "EQUALS", "value": "1"}],
                            'attrs': [None, 'y'],
                            'num': 100,
                            'start': 0,
                            'sort': ['x', None]}
        with self.assertRaises(TypeError): self.session._build_api_url(base_url, possible_options)


if __name__ == '__main__':
    unittest.main()
