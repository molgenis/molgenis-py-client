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

    def test_login_and_get_superuser_username(self):
        s = molgenis.Session(self.api_url)
        s.login('admin', 'admin')
        response = s.get(self.user_entity, q=[{"field": "superuser", "operator": "EQUALS", "value": "true"}])
        self.assertEqual('admin', response[0]['username'])
        s.logout()

    def test_login_logout_and_get_MolgenisUser(self):
        s = molgenis.Session(self.api_url)
        s.login('admin', 'admin')
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
            print(str(e))
        response = self.session.add_all(self.ref_entity,
                             [{"value": "ref55", "label": "label55"}, {"value": "ref57", "label": "label57"}])
        self.assertEqual(['ref55', 'ref57'], response)
        item55 = self.session.get(self.ref_entity, q=[{"field": "value", "operator": "EQUALS", "value": "ref55"}])[0]
        self.assertEqual({"value": "ref55", "label": "label55", "href": "/api/v1/" + self.ref_entity + "/ref55"},
                         item55)
        self.session.delete(self.ref_entity, 'ref55')
        self.session.delete(self.ref_entity, 'ref57')

    def test_add_dict(self):
        try:
            self.session.delete(self.ref_entity, 'ref55')
        except Exception as e:
            print(str(e))
        self.assertEqual('ref55', self.session.add(self.ref_entity, {"value": "ref55", "label": "label55"}))
        self.session.delete(self.ref_entity, 'ref55')

    def test_update_one(self):
        try:
            self.session.delete(self.ref_entity, 'ref55')
        except Exception as e:
            print(str(e))
        self.assertEqual('ref55', self.session.add(self.ref_entity, {"value": "ref55", "label": "label55"}))
        try:
            self.session.update_one(self.ref_entity, 'ref55', 'label', 'updated-label55');
        except Exception as e:
            print(str(e))
        item55 = self.session.get_by_id(self.ref_entity, "ref55", ["label"])
        self.assertEqual("updated-label55", item55["label"])
        self.session.delete(self.ref_entity, 'ref55')

    def test_add_kwargs(self):
        try:
            self.session.delete(self.ref_entity, 'ref55')
        except Exception as e:
            print(str(e))
        self.assertEqual('ref55', self.session.add(self.ref_entity, value="ref55", label="label55"))
        item55 = self.session.get(self.ref_entity, q=[{"field": "value", "operator": "EQUALS", "value": "ref55"}])[0]
        self.assertEqual({"value": "ref55", "label": "label55", "href": "/api/v1/" + self.ref_entity + "/ref55"},
                         item55)
        self.session.delete(self.ref_entity, 'ref55')

    def test_add_merge_dict_kwargs(self):
        try:
            self.session.delete(self.ref_entity, 'ref55')
        except Exception as e:
            print(str(e))
        self.assertEqual('ref55', self.session.add(self.ref_entity, {"value": "ref55"}, label="label55"))
        item55 = self.session.get(self.ref_entity, q=[{"field": "value", "operator": "EQUALS", "value": "ref55"}])[0]
        self.assertEqual({"value": "ref55", "label": "label55", "href": "/api/v1/" + self.ref_entity + "/ref55"},
                         item55)
        self.session.delete(self.ref_entity, 'ref55')

    def test_get_meta(self):
        meta = self.session.get_entity_meta_data(self.user_entity)
        self.assertEqual('username', meta['labelAttribute'])

    def test_get_attribute_meta(self):
        meta = self.session.get_attribute_meta_data(self.user_entity, 'username')
        self.assertEqual({'labelAttribute': True, 'isAggregatable': False, 'name': 'username',
                          'auto': False, 'nillable': False, 'label': 'Username', 'lookupAttribute': True,
                          'visible': True, 'readOnly': True, 'href': '/api/v1/sys_sec_User/meta/username',
                          'enumOptions': [], 'fieldType': 'STRING', 'maxLength': 255, 'attributes': [],
                          'unique': True},
                         meta)


if __name__ == '__main__':
    unittest.main()
