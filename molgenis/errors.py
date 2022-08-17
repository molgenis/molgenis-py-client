import json


class MolgenisRequestError(Exception):
    def __init__(self, error, response=False):
        self.message = error
        if response:
            self.response = response


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
