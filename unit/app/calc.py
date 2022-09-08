
def calculator(expression):
    allowed_ops = '-+/*'
    if not any(sign in allowed_ops for sign in expression):
        raise ValueError(f'Выражение должно содержать хотя бы один знак ({allowed_ops})')
    for sign in allowed_ops:
        if sign in expression:
            try:
                left, right = expression.split(sign)
                left, right = int(left), int(right)
                return {
                    '+': lambda a, b: a+b,
                    '-': lambda a, b: a-b,
                    '/': lambda a, b: a/b,
                    '*': lambda a, b: a*b,
                }[sign](left, right)
            except (ZeroDivisionError, ValueError, TypeError):
                raise ValueError('Выражение должно содержать два целых числа')


if __name__ == '__main__':
    result = calculator('1/1')
    print(result)
