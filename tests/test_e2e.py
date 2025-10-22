import asyncio
import json
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from httpx import AsyncClient

pytestmark = pytest.mark.e2e


BASE_DIR = Path(__file__).parent.parent


@pytest.mark.asyncio
async def test_create_rate(
    async_insurance_client: AsyncClient,
    async_log_client: AsyncClient,
) -> None:
    create_json = {
        "date": "2001-04-02",
        "cargo_type": "Glass",
        "rate": 0.002,
    }
    log_time = datetime.now()
    insurance_response = await async_insurance_client.post(
        url="/v1/rates/",
        json=create_json,
    )
    assert insurance_response.status_code == 201

    await asyncio.sleep(11)
    log_response = await async_log_client.get(
        url="/v1/logs/",
        params={"action": "create_insurance_rate"},
    )
    assert log_response.status_code == 200
    assert log_response.json()[0]["user_id"] is None
    assert log_response.json()[0]["action"] == "create_insurance_rate"
    insurance_response_time = datetime.fromisoformat(
        log_response.json()[0]["date_time"]
    ) + timedelta(hours=3)
    time_delta = timedelta(seconds=2)
    assert abs(insurance_response_time - log_time) < time_delta


@pytest.mark.asyncio
async def test_update_rate(
    async_insurance_client: AsyncClient,
    async_log_client: AsyncClient,
    test_rate: dict,
) -> None:
    update_json = {
        "date": "2002-04-02",
        "cargo_type": "Glass",
        "rate": 0.002,
    }
    log_time = datetime.now()
    insurance_response = await async_insurance_client.put(
        url=f"/v1/rates/{int(test_rate["id"])}/",
        json=update_json,
    )
    assert insurance_response.status_code == 200

    await asyncio.sleep(11)
    log_response = await async_log_client.get(
        url="/v1/logs/",
        params={"action": "update_insurance_rate"},
    )
    assert log_response.status_code == 200

    assert log_response.json()[0]["user_id"] is None
    assert log_response.json()[0]["action"] == "update_insurance_rate"
    insurance_response_time = datetime.fromisoformat(
        log_response.json()[0]["date_time"]
    ) + timedelta(hours=3)
    time_delta = timedelta(seconds=2)
    assert abs(insurance_response_time - log_time) < time_delta


@pytest.mark.asyncio
async def test_update_rate_partial(
    async_insurance_client: AsyncClient,
    async_log_client: AsyncClient,
    test_rate: dict,
) -> None:
    update_json = {
        "date": "2003-04-02",
        "cargo_type": "Glass",
    }
    log_time = datetime.now()
    insurance_response = await async_insurance_client.patch(
        url=f"/v1/rates/{int(test_rate["id"])}/",
        json=update_json,
    )
    assert insurance_response.status_code == 200

    await asyncio.sleep(11)
    request_datetime = log_time - timedelta(hours=3, seconds=2)
    log_response = await async_log_client.get(
        url="/v1/logs/",
        params={
            "action": "update_insurance_rate",
            "start_datetime": request_datetime.strftime("%Y-%m-%d %H:%M:%S"),
        },
    )
    assert log_response.status_code == 200
    assert log_response.json()[0]["user_id"] is None
    assert log_response.json()[0]["action"] == "update_insurance_rate"
    insurance_response_time = datetime.fromisoformat(
        log_response.json()[0]["date_time"]
    ) + timedelta(hours=3)
    time_delta = timedelta(seconds=2)
    assert abs(insurance_response_time - log_time) < time_delta


@pytest.mark.asyncio
async def test_get_a_few_logs(
    async_log_client: AsyncClient,
) -> None:
    log_response = await async_log_client.get(
        url="/v1/logs/",
        params={
            "action": "update_insurance_rate",
        },
    )
    assert log_response.status_code == 200
    assert len(log_response.json()) == 2


@pytest.mark.asyncio
async def test_delete_rate(
    async_insurance_client: AsyncClient,
    async_log_client: AsyncClient,
    test_rate: dict,
) -> None:
    delete_id = test_rate["id"]
    log_time = datetime.now()
    insurance_response = await async_insurance_client.delete(
        url=f"/v1/rates/{delete_id}/",
    )
    assert insurance_response.status_code == 204

    await asyncio.sleep(11)

    request_datetime = log_time - timedelta(hours=3, seconds=2)
    log_response = await async_log_client.get(
        url="/v1/logs/",
        params={
            "action": "delete_insurance_rate",
            "start_datetime": request_datetime.strftime("%Y-%m-%d %H:%M:%S"),
        },
    )
    assert log_response.status_code == 200
    assert log_response.json()[0]["user_id"] is None
    assert log_response.json()[0]["action"] == "delete_insurance_rate"
    insurance_response_time = datetime.fromisoformat(
        log_response.json()[0]["date_time"]
    ) + timedelta(hours=3)
    time_delta = timedelta(seconds=2)
    assert abs(insurance_response_time - log_time) < time_delta


@pytest.mark.asyncio
async def test_get_insurance_rate_for_calc(
    async_insurance_client: AsyncClient,
    async_log_client: AsyncClient,
    test_rate: dict,
) -> None:
    user_id = 18
    request_json = {
        "user_id": user_id,
        "date": test_rate["date"],
        "cargo_type": test_rate["cargo_type"],
        "declared_value": 3000,
    }
    log_time = datetime.now()
    insurance_response = await async_insurance_client.get(
        url="/v1/insurance_calculation/",
        params=request_json,
    )
    assert insurance_response.status_code == 200

    await asyncio.sleep(11)
    log_response = await async_log_client.get(
        url="/v1/logs/",
        params={"action": "get_insurance_rate_for_calc"},
    )
    assert log_response.status_code == 200
    assert log_response.json()[0]["user_id"] == user_id
    assert log_response.json()[0]["action"] == "get_insurance_rate_for_calc"
    insurance_response_time = datetime.fromisoformat(
        log_response.json()[0]["date_time"]
    ) + timedelta(hours=3)
    time_delta = timedelta(seconds=2)
    assert abs(insurance_response_time - log_time) < time_delta


@pytest.mark.asyncio
async def test_import_rates(
    async_insurance_client: AsyncClient,
    async_log_client: AsyncClient,
) -> None:
    rates_json = {
        "2023-11-30": [
            {"cargo_type": "Glass", "rate": "0.015"},
            {"cargo_type": "Other", "rate": "0.04"},
        ],
        "2023-12-01": [
            {"cargo_type": "Glass", "rate": "0.01"},
            {"cargo_type": "Other", "rate": "0.05"},
        ],
    }
    rates_file = BASE_DIR / "data" / "import_rates.json"
    rates_file.write_text(json.dumps(rates_json), encoding="utf-8")

    log_time = datetime.now()
    insurance_response = await async_insurance_client.get(
        url="/v1/administration/import_rates/",
    )
    assert insurance_response.status_code == 204

    await asyncio.sleep(11)
    log_response = await async_log_client.get(
        url="/v1/logs/",
        params={"action": "bulk_load_rates"},
    )
    assert log_response.status_code == 200
    assert log_response.json()[0]["user_id"] is None
    assert log_response.json()[0]["action"] == "bulk_load_rates"
    insurance_response_time = datetime.fromisoformat(
        log_response.json()[0]["date_time"]
    ) + timedelta(hours=3)
    time_delta = timedelta(seconds=2)
    assert abs(insurance_response_time - log_time) < time_delta
