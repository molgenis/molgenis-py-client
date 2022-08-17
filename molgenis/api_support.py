from enum import Enum
from dataclasses import dataclass, field
from http.cookiejar import CookiePolicy


class BlockAll(CookiePolicy):
    def return_ok(self, cookie, request):
        """Overwrite parent method"""
        pass

    def set_ok(self, cookie, request):
        return False


@dataclass(frozen=True)
class Headers:
    """
    This class is responsible for creating 'x-molgenis-token' headers
    for the current session
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
                               {"x-molgenis-token": self.token,
                                "Content-Type": "application/json"})
        else:
            object.__setattr__(self, "token_header", {})
            object.__setattr__(self, "ct_token_header", {})


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
