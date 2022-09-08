import pytest


def test_customer_not_exist(service):
    exist = service.is_exist("Nazar")
    # breakpoint() TODO: if need to check variables uncomment breakpoint!
    assert not exist


def test_remove_user(service):
    with pytest.raises(Exception) as err:
        service.remove_customer("Nazar")
    assert str(err.value) == "Customer is not exist!"


def test_create_user(service):
    with pytest.raises(Exception) as err:
        service.create_customer("Kevin")
    assert str(err.value) == "User already exists!"
