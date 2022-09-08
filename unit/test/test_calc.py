from unittest import TestCase, main
from app.calc import calculator


class TestCalculator(TestCase):

    def test_plus(self):
        self.assertEqual(calculator('2+2'), 4)

    def test_minus(self):
        self.assertEqual(calculator('3-1'), 2)

    def test_mul(self):
        self.assertEqual(calculator('5*5'), 25)

    def test_zero_div(self):
        with self.assertRaises(ValueError):
            calculator('1/0')

    def test_val_err(self):
        self.assertRaises(ValueError, calculator, 'fsfsdfs')


if __name__ == '__main__':
    main()
