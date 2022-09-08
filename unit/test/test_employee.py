from unittest import TestCase, main
from unittest.mock import patch

from app.employee import Employee


class TestEmployee(TestCase):

    def setUp(self) -> None:
        self.employee = Employee()

    def test_firstname(self):
        self.employee.first = 'John'
        self.assertEqual(self.employee.first, 'John')

    def test_lastname(self):
        self.employee.last = 'Uels'
        self.assertEqual(self.employee.last, 'Uels')

    def test_pay(self):
        self.employee.pay = 999
        self.assertEqual(self.employee.pay, 999)

    def test_mail(self):
        self.employee.first = 'Kevin'
        self.employee.last = 'Shindel'
        self.assertEqual(self.employee.email, 'Kevin.Shindel@mail.com')

    def test_fee(self):
        self.employee.pay = 999
        self.assertEqual(self.employee.fee, 999*0.15)

    @patch('app.employee.urlopen')
    def test_month_order_url(self, mock_urlopen):
        month = 'April'
        self.employee.month_order(month=month)
        url = f'http://www.company.com/orders/{self.employee.email}/{month}'
        mock_urlopen.assert_called_with(url=url)

    @patch('app.employee.urlopen')
    def test_success_month_order(self, mock_urlopen):
        mock_urlopen.return_value.status = 200
        mock_urlopen.return_value.text = 'Success'
        month = 'April'
        self.assertEqual(self.employee.month_order(month=month), 'Success')

    @patch('app.employee.urlopen')
    def test_fail_month_order(self, mock_urlopen):
        mock_urlopen.return_value.status = 403
        month = 'April'
        self.assertEqual(self.employee.month_order(month=month), 'Bad Response')


if __name__ == '__main__':
    main()
