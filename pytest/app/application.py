class Customer:

    def __init__(self):
        self.__customers = []

    def is_exist(self, name):
        return name in self.__customers

    def create_customer(self, name):
        if not self.is_exist(name):
            self.__customers.append(name)
        else:
            raise Exception("User already exists!")

    def get_customers(self):
        return self.__customers

    def remove_customer(self, name):
        if self.is_exist(name):
            self.__customers.remove(name)
        else:
            raise Exception("Customer is not exist!")
