from unittest import TestCase, main, mock

from app.sql import User, make_query


class TestSql(TestCase):

    def test_sql(self):
        test_user = User(id=1, name='kevin')
        sql_user: User = make_query('kevin')
        self.assertEqual(test_user, sql_user)

    @mock.patch('app.sql.connect')
    def test_make_query(self, connect_mock):
        engine_mock = connect_mock.return_value
        cursor_mock = engine_mock.cursor.return_value
        cursor_mock.execute.return_value.fetchone.return_value = (1, 'kevin')
        test_user = User(id=1, name='kevin')
        sql_user: User = make_query('kevin')
        self.assertEqual(test_user, sql_user)

    @mock.patch('app.sql.connect')
    def test_sql_query_calls(self, connect_mock):
        ''' create engine mock'''
        engine_mock = connect_mock.return_value
        ''' create cursor mock '''
        cursor_mock = engine_mock.cursor.return_value
        ''' provide return value '''
        cursor_mock.execute.return_value.fetchone.return_value = (1, 'kevin')
        # run function
        make_query('kevin')
        # check cursor called with sql query
        cursor_mock.execute.assert_called_with("SELECT * FROM test_table WHERE name = 'kevin'")


if __name__ == '__main__':
    main()
