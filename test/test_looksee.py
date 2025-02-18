import pytest
from src.looksee import LookSee
from pathlib import Path

SAMPLE_DATA_CSV = Path.cwd() / "test" / "sample_test_data.csv"


@pytest.fixture
def looksee_instance():
    return LookSee()


def test_ingest_data(looksee_instance):
    assert looksee_instance.ingest_data(SAMPLE_DATA_CSV) is True


def test_extract_metadata(looksee_instance):
    looksee_instance.ingest_data(SAMPLE_DATA_CSV)
    looksee_instance.extract_metadata()
    assert len(looksee_instance.metadata) > 0


def test_column_summary(looksee_instance):
    looksee_instance.ingest_data(SAMPLE_DATA_CSV)
    summary = looksee_instance.column_summary("age")
    assert "min_value" in summary and "max_value" in summary
