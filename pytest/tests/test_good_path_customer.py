

def test_customer_exist(service):
    exist = service.is_exist("Kevin")
    assert exist, True


def test_create_customer(service):
    service.create_customer("Nazar")
    is_exist = service.is_exist("Nazar")
    customers = service.get_customers()
    assert len(customers) == 4
    assert is_exist, True


def test_remove_user(service):
    service.remove_customer("Kevin")
    is_exist = service.is_exist("Kevin")
    customers = service.get_customers()
    assert len(customers) == 2
    assert not is_exist


