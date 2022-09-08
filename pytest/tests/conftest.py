import pytest
from test.app import Customer


@pytest.fixture(scope="function")
def service():
    """
    Fixtures are created when first requested by a test, and are destroyed based on their scope:
    function: the default scope, the fixture is destroyed at the end of the test.
    class: the fixture is destroyed during teardown of the last test in the class.
    module: the fixture is destroyed during teardown of the last test in the module.
    package: the fixture is destroyed during teardown of the last test in the package.
    session: the fixture is destroyed at the end of the test session.
    """
    customers = ["Kevin", "Marta", "Zahar"]
    service = Customer()
    for c in customers:
        service.create_customer(c)
    yield service
