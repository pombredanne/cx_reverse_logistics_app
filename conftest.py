"""Setting Testing Conftest."""
import pytest


def pytest_addoption(parser):
    """Pytest Add Option."""
    parser.addoption("--stage", action="store", default="dev")
    parser.addoption("--env", action="store", default="dev")


@pytest.fixture
def stage(request):
    """Stage Setting."""
    return request.config.getoption("--stage")


@pytest.fixture
def env(request):
    """Env Setting."""
    return request.config.getoption("--env")
