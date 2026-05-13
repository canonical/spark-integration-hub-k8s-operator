# Copyright 2026 Canonical Limited
# See LICENSE file for licensing details.

from dataclasses import dataclass
from unittest import mock

from botocore.exceptions import ClientError, SSLError

from managers.s3 import S3Manager


@dataclass
class S3ConnectionInfoTester:
    access_key: str = "access-key"
    secret_key: str = "secret-key"
    bucket: str = "my-bucket"
    path: str = "logs"
    region: str = "us-east-1"
    endpoint: str = "https://s3.amazonaws.com"
    tls_ca_chain: str = ""


def _client_error(code: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": "test"}}, "operation")


def _make_manager(info: S3ConnectionInfoTester) -> tuple[S3Manager, mock.MagicMock]:
    """Return an S3Manager wired to a mocked S3 client."""
    manager = S3Manager(info)
    mock_s3 = mock.MagicMock()
    mock_session = mock.MagicMock()
    mock_session.client.return_value = mock_s3
    manager.__dict__["session"] = mock_session
    return manager, mock_s3


def test_verify_bucket_and_path_exist():
    """Bucket and path exist — list and write both succeed, returns True."""
    manager, mock_s3 = _make_manager(S3ConnectionInfoTester())

    assert manager.verify() is True
    mock_s3.list_objects_v2.assert_called_once_with(Bucket="my-bucket", Prefix="logs/", MaxKeys=1)
    mock_s3.put_object.assert_called_once_with(Bucket="my-bucket", Key="logs/", Body=b"")
    mock_s3.create_bucket.assert_not_called()


def test_verify_bucket_exists_path_empty():
    """Bucket exists but path is empty — put_object creates the marker, returns True."""
    manager, mock_s3 = _make_manager(S3ConnectionInfoTester())

    assert manager.verify() is True
    mock_s3.list_objects_v2.assert_called_once_with(Bucket="my-bucket", Prefix="logs/", MaxKeys=1)
    mock_s3.put_object.assert_called_once_with(Bucket="my-bucket", Key="logs/", Body=b"")
    mock_s3.create_bucket.assert_not_called()


def test_verify_bucket_missing_creates_bucket_and_path():
    """Bucket does not exist (us-east-1) — creates bucket and path marker, returns True."""
    manager, mock_s3 = _make_manager(S3ConnectionInfoTester())
    mock_s3.list_objects_v2.side_effect = [
        _client_error("NoSuchBucket"),  # attempt 0: bucket missing
        {},  # attempt 1: bucket now exists
    ]

    assert manager.verify() is True
    mock_s3.create_bucket.assert_called_once_with(Bucket="my-bucket")
    mock_s3.put_object.assert_called_once_with(Bucket="my-bucket", Key="logs/", Body=b"")


def test_verify_bucket_missing_non_us_east_1_region():
    """Bucket missing in eu-west-1 — creates with LocationConstraint, returns True."""
    info = S3ConnectionInfoTester(region="eu-west-1")
    manager, mock_s3 = _make_manager(info)
    mock_s3.list_objects_v2.side_effect = [
        _client_error("NoSuchBucket"),
        {},
    ]

    assert manager.verify() is True
    mock_s3.create_bucket.assert_called_once_with(
        Bucket="my-bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
    )
    mock_s3.put_object.assert_called_once_with(Bucket="my-bucket", Key="logs/", Body=b"")


def test_verify_composite_path():
    """Hierarchical path (logs/dev1/dev2) — list and write use the full path, returns True."""
    info = S3ConnectionInfoTester(path="logs/dev1/dev2")
    manager, mock_s3 = _make_manager(info)

    assert manager.verify() is True
    mock_s3.list_objects_v2.assert_called_once_with(
        Bucket="my-bucket", Prefix="logs/dev1/dev2/", MaxKeys=1
    )
    mock_s3.put_object.assert_called_once_with(Bucket="my-bucket", Key="logs/dev1/dev2/", Body=b"")


def test_verify_read_but_no_write():
    """Read access exists but write is denied — returns False."""
    manager, mock_s3 = _make_manager(S3ConnectionInfoTester())
    mock_s3.put_object.side_effect = _client_error("AccessDenied")

    assert manager.verify() is False
    mock_s3.list_objects_v2.assert_called_once()
    mock_s3.put_object.assert_called_once()
    mock_s3.create_bucket.assert_not_called()


def test_verify_no_create_bucket_permission():
    """Bucket missing and no permission to create it — returns False."""
    manager, mock_s3 = _make_manager(S3ConnectionInfoTester())
    mock_s3.list_objects_v2.side_effect = _client_error("NoSuchBucket")
    mock_s3.create_bucket.side_effect = _client_error("AccessDenied")

    assert manager.verify() is False
    mock_s3.create_bucket.assert_called_once()
    mock_s3.put_object.assert_not_called()


def test_verify_no_read_permission():
    """No permission to list bucket — returns False immediately."""
    manager, mock_s3 = _make_manager(S3ConnectionInfoTester())
    mock_s3.list_objects_v2.side_effect = _client_error("AccessDenied")

    assert manager.verify() is False
    mock_s3.put_object.assert_not_called()
    mock_s3.create_bucket.assert_not_called()


def test_verify_permanent_redirect_wrong_region():
    """Bucket exists in a different region than the configured endpoint — returns False."""
    manager, mock_s3 = _make_manager(S3ConnectionInfoTester(region="eu-west-1"))
    mock_s3.list_objects_v2.side_effect = _client_error("PermanentRedirect")

    assert manager.verify() is False
    mock_s3.put_object.assert_not_called()
    mock_s3.create_bucket.assert_not_called()


def test_verify_ssl_error():
    """SSL certificate validation failure — returns False."""
    manager, mock_s3 = _make_manager(S3ConnectionInfoTester())
    mock_s3.list_objects_v2.side_effect = SSLError(
        endpoint_url="https://s3.amazonaws.com", error="certificate verify failed"
    )

    assert manager.verify() is False
    mock_s3.put_object.assert_not_called()
    mock_s3.create_bucket.assert_not_called()
