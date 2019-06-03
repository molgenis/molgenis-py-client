[![Quality Status](https://sonarcloud.io/api/project_badges/measure?project=org.molgenis%3Apython-client&metric=alert_status)](https://sonarcloud.io/dashboard?id=org.molgenis%3Apython-client)

# molgenis-py-client
A Python client for the MOLGENIS REST API

### How to install

In a terminal, run:

```
pip install molgenis-py-client
```

### Development
Want to help out? Fork and clone this repository, go to the root of the project and create a virtual environment (requires
Python 3.7 or higher):

```
python -m virtualenv env
```

Now activate the environment. How to do this depends on your operating system, read 
[the virtualenv docs](https://virtualenv.pypa.io/en/latest/userguide) for more info. 
The following example assumes MacOS:


```
source env/bin/activate
```

Then install the project in development mode:
```
pip install -e .
```

If you want to leave the environment, use `deactivate`.

### Using the API client
Documentation for the usage of this API can be found in the
[MOLGENIS documentation](https://molgenis.gitbooks.io/molgenis/content/developer_documentation/ref-python.html).

### Running the tests
The tests run against a running molgenis on `localhost:8080`. To change this, alter the URL on line 11 in the
`test_client.py` file. To run the tests, run the following command:
```
python setup.py test
```
Alternatively, run the tests by clicking the run-button in PyCharm.