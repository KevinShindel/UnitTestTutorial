from collections import namedtuple
from sqlite3 import connect


User = namedtuple('User', 'id name')


def make_query(name):
    engine = connect(':memory:')

    cursor = engine.cursor()
    cursor.execute('CREATE TABLE test_table (ID INT NOT NULL, NAME VARCHAR NOT NULL)')
    cursor.execute('INSERT INTO test_table VALUES (1, "kevin"), (2, "shindel")')

    response = cursor.execute(f"SELECT * FROM test_table WHERE name = '{name}'")
    return User(*response.fetchone())


if __name__ == '__main__':
    res = make_query('kevin')
    print(res)
