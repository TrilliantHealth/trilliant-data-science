from azure.core import MatchConditions
from azure.storage.filedatalake import FileProperties


def match_etag(file_properties: FileProperties) -> dict:
    return dict(etag=file_properties.etag, match_condition=MatchConditions.IfNotModified)
