import time
from pathlib import Path
from typing import AsyncGenerator, Generator

import pytest
import pytest_asyncio
from httpx import AsyncClient
from testcontainers.compose import ComposeContainer, DockerCompose
from testcontainers.core.wait_strategies import LogMessageWaitStrategy

PROJECT_PATH = Path(__file__).parent.parent


@pytest.fixture(scope="session")
def compose_containers() -> Generator[DockerCompose, None, None]:
    compose = DockerCompose(context=PROJECT_PATH, wait=False)
    compose.waiting_for(
        {
            "pg_calc": LogMessageWaitStrategy(
                "database system is ready to accept connections"
            ),
            "pg_logger": LogMessageWaitStrategy(
                "database system is ready to accept connections"
            ),
            "insurance_calc_ms": LogMessageWaitStrategy("Application startup complete"),
            "logger_ms": LogMessageWaitStrategy("Application startup complete"),
        }
    )
    try:
        compose.stop()
        time.sleep(4)
        compose.start()
        yield compose
    finally:
        compose.stop()


@pytest.fixture(scope="session")
def insurance_container(compose_containers: DockerCompose) -> str:
    app: ComposeContainer = compose_containers.get_container("insurance_calc_ms")
    host = app.get_container_host_ip()
    port = app.get_exposed_port(port=8000)
    return f"http://{host}:{port}"


@pytest.fixture(scope="session")
def log_container(compose_containers: DockerCompose) -> str:
    app: ComposeContainer = compose_containers.get_container("logger_ms")
    host = app.get_container_host_ip()
    port = app.get_exposed_port(port=8050)
    return f"http://{host}:{port}"


@pytest_asyncio.fixture(scope="function")
async def async_insurance_client(
    insurance_container: str,
) -> AsyncGenerator[AsyncClient, None]:
    async with AsyncClient(
        base_url=f"{insurance_container}/api",
        follow_redirects=False,
        headers={"Cache-Control": "no-cache"},
    ) as ac:
        yield ac


@pytest_asyncio.fixture(scope="function")
async def async_log_client(log_container: str) -> AsyncGenerator[AsyncClient, None]:
    async with AsyncClient(
        base_url=f"{log_container}/api",
        follow_redirects=False,
        headers={"Cache-Control": "no-cache"},
    ) as ac:
        yield ac


@pytest_asyncio.fixture
async def test_rate(async_insurance_client: AsyncClient) -> AsyncGenerator[dict, None]:
    create_json = {
        "date": "2021-04-02",
        "cargo_type": "Beer",
        "rate": 0.005,
    }
    response = await async_insurance_client.post(
        url="/v1/rates/",
        json=create_json,
    )
    yield response.json()
    await async_insurance_client.delete(
        url=f"/v1/rates/{response.json()["id"]}/",
    )
