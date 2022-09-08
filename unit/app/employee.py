from urllib.request import urlopen


class Employee:

    def __init__(self, first='Kevin', last='Shindel', pay=100):
        self.first = first
        self.last = last
        self.pay = pay
        self._fee = 0.15

    @property
    def email(self):
        return f'{self.first}.{self.last}@mail.com'

    @property
    def fee(self):
        return self.pay * self._fee

    def month_order(self, month):
        url = f'http://www.company.com/orders/{self.email}/{month}'
        response = urlopen(url=url)
        if response.status == 200:
            return response.text
        else:
            return 'Bad Response'


if __name__ == '__main__':
    Employee().month_order('May')

