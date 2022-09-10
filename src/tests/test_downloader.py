import pytest
import requests_mock
from src.pysec_downloader.downloader import Downloader, SEC_BULK_SUBMISSIONS
import zipfile
from io import BytesIO
from pathlib import Path

@pytest.fixture
def get_zip_file(tmp_path):
    temp = tmp_path
    temp_zip = Path(temp) / "test.zip"
    test_string = "this will become a zip file"
    with zipfile.ZipFile(temp_zip, 'w', compression=zipfile.ZIP_DEFLATED) as z:
        z.writestr("file1.txt", test_string)
    yield temp_zip


def test_zip_file_download_with_extract(tmp_path, get_zip_file):
    root_path = tmp_path
    zip_file_path = get_zip_file
    dl = Downloader(
        root_path=root_path,
        user_agent="test requests mock@this.com")
    with requests_mock.Mocker() as m:
        with open(zip_file_path, "rb") as f:
            m.get(SEC_BULK_SUBMISSIONS, content=f.read(), headers={"Content-Length": "1000"})
            dl._handle_download_zip_file_with_extract(url=SEC_BULK_SUBMISSIONS, extract_path=(root_path / "submissions"))
        result_dir = root_path / "submissions"
        assert "file1.txt" in [i.parts[-1] for i in result_dir.glob("*")]
        assert (root_path / "temp.zip").exists() is False
    

def test_zip_file_download_without_extract(tmp_path, get_zip_file):
    root_path = tmp_path
    zip_file_path = get_zip_file
    dl = Downloader(
        root_path=root_path,
        user_agent="test requests mock@this.com")
    with requests_mock.Mocker() as m:
        with open(zip_file_path, "rb") as f:
            m.get(SEC_BULK_SUBMISSIONS, content=f.read(), headers={"Content-Length": "1000"})
            dl._handle_download_zip_file_without_extract(url=SEC_BULK_SUBMISSIONS, save_path=(root_path / "submissions.zip"))
        result_file = root_path / "submissions.zip"
        assert result_file.exists() is True
        assert (root_path / "temp.zip").exists() is False
