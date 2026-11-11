import json
from unittest.mock import MagicMock, patch

import pytest
import requests

from src.ingestion.api_ingestion import API_URL, ProductAPIClient

# ============================================================
# TEST 1 : Configuration HTTP
# ============================================================


@pytest.mark.parametrize(
    "status_code",
    [429, 500, 502, 503, 504],
)
def test_create_session_retry_status_codes(status_code):

    client = ProductAPIClient()
    session = client.create_session()

    try:
        assert isinstance(session, requests.Session)

        adapter = session.get_adapter(API_URL)
        retry = adapter.max_retries

        assert retry.total == 3
        assert retry.backoff_factor == 1
        assert status_code in retry.status_forcelist

    finally:
        session.close()


# ============================================================
# TEST 2 : Pagination - Plusieurs scénarios
# ============================================================


@pytest.mark.parametrize(
    "pages, expected_ids, expected_calls",
    [
        pytest.param(
            [
                {"products": [{"id": 1}, {"id": 2}], "total": 3},
                {"products": [{"id": 3}], "total": 3},
            ],
            [1, 2, 3],
            2,
            id="pagination_deux_pages",
        ),
        pytest.param(
            [
                {"products": [{"id": 1}, {"id": 2}], "total": 2},
            ],
            [1, 2],
            1,
            id="pagination_une_page",
        ),
        pytest.param(
            [
                {"products": [], "total": 0},
            ],
            [],
            1,
            id="aucun_produit",
        ),
    ],
)
@patch("src.ingestion.api_ingestion.ProductAPIClient.create_session")
def test_extract_products_pagination(
    mock_create_session,
    pages,
    expected_ids,
    expected_calls,
):

    responses = []

    for page in pages:
        response = MagicMock()
        response.json.return_value = page
        responses.append(response)

    session = MagicMock()
    session.get.side_effect = responses

    mock_create_session.return_value.__enter__.return_value = session

    client = ProductAPIClient()
    result = client.extract_products()

    assert result["total"] == len(expected_ids)

    assert [product["id"] for product in result["products"]] == expected_ids

    assert session.get.call_count == expected_calls

    for call_index in range(expected_calls):
        expected_skip = sum(len(page["products"]) for page in pages[:call_index])

        session.get.assert_any_call(
            API_URL,
            params={
                "limit": 100,
                "skip": expected_skip,
            },
            timeout=30,
        )


# ============================================================
# TEST 3 : Validation des réponses API
# ============================================================


@pytest.mark.parametrize(
    "api_response, expected_error",
    [
        pytest.param(
            ["format", "incorrect"],
            "Format JSON inattendu",
            id="json_invalide",
        ),
        pytest.param(
            {"total": 10},
            "Liste de produits absente",
            id="products_absent",
        ),
        pytest.param(
            {"products": "incorrect", "total": 10},
            "Liste de produits absente",
            id="products_pas_une_liste",
        ),
        pytest.param(
            {"products": [{"id": 1}], "total": -1},
            "Total de produits invalide",
            id="total_negatif",
        ),
        pytest.param(
            {"products": [{"id": 1}], "total": None},
            "Total de produits invalide",
            id="total_null",
        ),
        pytest.param(
            {"products": [], "total": 10},
            "Page vide",
            id="page_vide_avant_fin",
        ),
    ],
)
@patch("src.ingestion.api_ingestion.ProductAPIClient.create_session")
def test_extract_products_invalid_responses(
    mock_create_session,
    api_response,
    expected_error,
):

    response = MagicMock()
    response.json.return_value = api_response

    session = MagicMock()
    session.get.return_value = response

    mock_create_session.return_value.__enter__.return_value = session

    client = ProductAPIClient()

    with pytest.raises(ValueError, match=expected_error):
        client.extract_products()


# ============================================================
# TEST 4 : Erreurs HTTP
# ============================================================


@pytest.mark.parametrize(
    "status_code",
    [400, 401, 403, 404, 500, 503],
)
@patch("src.ingestion.api_ingestion.ProductAPIClient.create_session")
def test_extract_products_http_errors(
    mock_create_session,
    status_code,
):

    response = MagicMock()
    response.status_code = status_code

    response.raise_for_status.side_effect = requests.HTTPError(f"HTTP {status_code}")

    session = MagicMock()
    session.get.return_value = response

    mock_create_session.return_value.__enter__.return_value = session

    client = ProductAPIClient()

    with pytest.raises(requests.HTTPError):
        client.extract_products()


# ============================================================
# TEST 5 : Sauvegarde JSON Bronze
# ============================================================


@pytest.mark.parametrize(
    "products",
    [
        pytest.param(
            [{"id": 1, "title": "iPhone"}],
            id="un_produit",
        ),
        pytest.param(
            [
                {"id": 1, "title": "iPhone"},
                {"id": 2, "title": "Samsung"},
            ],
            id="plusieurs_produits",
        ),
        pytest.param(
            [],
            id="liste_vide",
        ),
    ],
)
def test_save_raw_data(tmp_path, products):

    client = ProductAPIClient(output_dir=tmp_path)

    data = {
        "products": products,
        "total": len(products),
    }

    filename = client.save_raw_data(data)

    assert filename.exists()
    assert filename.suffix == ".json"
    assert filename.parent == tmp_path

    with open(filename, encoding="utf-8") as file:
        saved_data = json.load(file)

    assert saved_data == data


# ============================================================
# TEST 6 : Vérifier l'exécution du pipeline
# ============================================================


@patch.object(ProductAPIClient, "save_raw_data")
@patch.object(ProductAPIClient, "extract_products")
def test_run_pipeline(
    mock_extract,
    mock_save,
):

    products = {
        "products": [{"id": 1}],
        "total": 1,
    }

    mock_extract.return_value = products

    client = ProductAPIClient()
    client.run()

    mock_extract.assert_called_once_with()
    mock_save.assert_called_once_with(products)
