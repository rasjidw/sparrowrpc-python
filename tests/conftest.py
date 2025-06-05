import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--start_port", action="store", default=65003, type=int, help="starting port to listen on"
    )


@pytest.fixture(scope="session")
def start_port(request):
    return request.config.getoption("--start_port")
