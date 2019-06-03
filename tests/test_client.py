import unittest

import molgenis.client as molgenis


class TestStringMethods(unittest.TestCase):
    """
    Tests the client against a running MOLGENIS.
    """

    api_url = "http://localhost:8080/api/"

    no_readmeta_permission_user_msg = "401 Client Error:  for url: {}v2/sys_sec_User: No 'Read metadata' " \
                                      "permission on entity type 'User' with id 'sys_sec_User'.".format(api_url)
    user_entity = 'sys_sec_User'
    ref_entity = 'org_molgenis_test_python_TypeTestRef'
    session = molgenis.Session(api_url)
    session.login('admin', 'admin')

    def _try_delete(self, entity_type, entity_ids):
        # Try to remove because if a previous test failed, possibly the refs you're about to add are not removed yet
        try:
            self.session.delete_list(entity_type, entity_ids)
        except Exception as e:
            print(e)

    def _try_add(self, entity_type, entities):
        # Try to add because if a previous test failed, possibly the refs you're about to reove are not added yet
        try:
            self.session.add_all(entity_type, entities)
        except Exception as e:
            print(e)

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
        s.login('admin', 'admin')
        s.get(self.user_entity)
        s.logout()
        try:
            s.get(self.user_entity)
        except Exception as e:
            message = e.args[0]
            self.assertEqual(self.no_readmeta_permission_user_msg, message)

    def test_no_login_and_get_MolgenisUser(self):
        s = molgenis.Session(self.api_url)
        try:
            s.get(self.user_entity)
        except Exception as e:
            message = e.args[0]
            self.assertEqual(self.no_readmeta_permission_user_msg, message)

    def test_upload_zip(self):
        self._try_delete('sys_md_EntityType', ['org_molgenis_test_python_sightings'])
        response = self.session.upload_zip('./resources/sightings_test.zip').split('/')
        run_entity_type = response[-2]
        run_id = response[-1]
        status_info = self.session.get_by_id(run_entity_type, run_id)
        while status_info['status'] == 'RUNNING':
            status_info = self.session.get_by_id(run_entity_type, run_id)
        self.assertEqual('FINISHED', status_info['status'])

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
        response = self.session.upload_zip('./resources/sightings_test.zip').split('/')
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
            expected = "400 Client Error:  for url: http://localhost:8080/api/v2/org_molgenis_test_python_TypeTest" \
                       "Ref: The attribute 'label' of entity 'org_molgenis_test_python_TypeTestRef' can not be null."
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
        item55 = self.session.get_by_id(self.ref_entity, "ref55", ["label"])
        self.assertEqual("updated-label55", item55["label"])
        self.session.delete(self.ref_entity, 'ref55')

    def test_update_one_error(self):
        try:
            self.session.update_one(self.ref_entity, 'ref555', 'label', 'updated-label555')
        except Exception as e:
            message = e.args[0]
            expected = "404 Client Error:  for url: http://localhost:8080/api/v1/org_molgenis_test_python_TypeTestRef" \
                       "/ref555/label: Unknown entity with 'value' 'ref555' of type 'TypeTestRef'."
            self.assertEqual(expected, message)

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
        expected = [{'_href': '/api/v2/org_molgenis_test_python_TypeTestRef/ref1', 'value': 'ref1', 'label': 'label1'},
                    {'_href': '/api/v2/org_molgenis_test_python_TypeTestRef/ref2', 'value': 'ref2', 'label': 'label2'},
                    {'_href': '/api/v2/org_molgenis_test_python_TypeTestRef/ref3', 'value': 'ref3', 'label': 'label3'},
                    {'_href': '/api/v2/org_molgenis_test_python_TypeTestRef/ref4', 'value': 'ref4', 'label': 'label4'},
                    {'_href': '/api/v2/org_molgenis_test_python_TypeTestRef/ref5', 'value': 'ref5', 'label': 'label5'}]
        self.assertEqual(expected, data)

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

    def test_get_expand(self):
        data = self.session.get(self.ref_entity.replace('Ref', ''), expand='xcomputedxref')
        first_item = data[0]
        expected = {"_href": "/api/v2/org_molgenis_test_python_Location/5", "Chromosome": "str1", "Position": 5}
        self.assertEqual(47, len(first_item))
        self.assertEqual(expected, first_item['xcomputedxref'])

    def test_get_expand_attrs(self):
        data = self.session.get(self.ref_entity.replace('Ref', ''), expand='xcomputedxref',
                                attributes='id,xcomputedxref')
        first_item = data[0]
        expected = {"_href": "/api/v2/org_molgenis_test_python_Location/5", "Chromosome": "str1", "Position": 5}
        self.assertEqual(len(first_item), 3)
        self.assertEqual(expected, first_item['xcomputedxref'])

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


if __name__ == '__main__':
    unittest.main()
