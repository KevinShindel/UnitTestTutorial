# Pytest in docker container
<img src="https://docs.pytest.org/en/7.1.x/_static/pytest_logo_curves.svg" alt="pytest logo" style="height: 100px; width:100px;"/>

1. Run build
```shell
docker-compose build
```
2. Run
```shell
docker-compose run test
```

3. Show coverage
```shell
docker-compose run test pytest --cov
```

4. Run test via prefix
```shell
docker-compose run test pytest -k test_prefix_example
```
