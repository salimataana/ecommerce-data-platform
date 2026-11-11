import json
from unittest.mock import MagicMock, patch

import pytest
import requests

from src.ingestion import api_ingestion


# TEST 1 : Configuration de la session HTTP
def test_create_session():

    session = api_ingestion.create_session()

    assert isinstance(session, requests.Session)

    adapter = session.get_adapter("https://dummyjson.com")

    retry = adapter.max_retries

    assert retry.total == 3
    assert retry.backoff_factor == 1
    assert 429 in retry.status_forcelist
    assert 500 in retry.status_forcelist

    session.close()


# TEST 2 : Pagination de l'API
@patch("src.ingestion.api_ingestion.create_session")
def test_extract_products_pagination(mock_create_session):

    first_page = {
        "products": [{"id": 1}, {"id": 2}],
        "total": 3,
    }

    second_page = {
        "products": [{"id": 3}],
        "total": 3,
    }

    response1 = MagicMock()
    response1.json.return_value = first_page

    response2 = MagicMock()
    response2.json.return_value = second_page

    session = MagicMock()
    session.get.side_effect = [response1, response2]

    mock_create_session.return_value.__enter__.return_value = session

    result = api_ingestion.extract_products()

    assert result["total"] == 3
    assert len(result["products"]) == 3

    assert result["products"][0]["id"] == 1
    assert result["products"][2]["id"] == 3

    assert session.get.call_count == 2

    session.get.assert_any_call(
        api_ingestion.API_URL,
        params={"limit": 100, "skip": 0},
        timeout=30,
    )

    session.get.assert_any_call(
        api_ingestion.API_URL,
        params={"limit": 100, "skip": 2},
        timeout=30,
    )


# TEST 3 : Format JSON invalide
@patch("src.ingestion.api_ingestion.create_session")
def test_extract_products_invalid_json(mock_create_session):

    response = MagicMock()
    response.json.return_value = ["format", "incorrect"]

    session = MagicMock()
    session.get.return_value = response

    mock_create_session.return_value.__enter__.return_value = session

    with pytest.raises(ValueError, match="Format JSON inattendu"):
        api_ingestion.extract_products()


# TEST 4 : Page vide avant la fin
@patch("src.ingestion.api_ingestion.create_session")
def test_extract_products_empty_page(mock_create_session):

    response = MagicMock()
    response.json.return_value = {
        "products": [],
        "total": 10,
    }

    session = MagicMock()
    session.get.return_value = response

    mock_create_session.return_value.__enter__.return_value = session

    with pytest.raises(ValueError, match="Page vide"):
        api_ingestion.extract_products()


# TEST 5 : Enregistrement JSON
def test_save_raw_data(tmp_path, monkeypatch):

    monkeypatch.setattr(
        api_ingestion,
        "OUTPUT_DIR",
        tmp_path,
    )

    data = {
        "products": [
            {"id": 1, "title": "iPhone"},
            {"id": 2, "title": "Samsung"},
        ],
        "total": 2,
    }

    filename = api_ingestion.save_raw_data(data)

    assert filename.exists()
    assert filename.suffix == ".json"

    with open(filename, encoding="utf-8") as file:
        saved_data = json.load(file)

    assert saved_data == data
    assert len(saved_data["products"]) == 2