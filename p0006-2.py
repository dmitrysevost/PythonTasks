class Animal:
    num_ins = 0

    def __init__(self):
        Animal.num_ins = Animal.num_ins + 1

    def get_num_ins():
        print(Animal.num_ins)

    get_num_ins = staticmethod(get_num_ins)

    def voice(self):
        pass


class SubAnimal1(Animal):
    def voice(self, a):
        print(a + '1')


class SubAnimal2(Animal):
    def voice(self, a):
        print(a + '2')


class SubAnimal3(Animal):
    def voice(self, a):
        print(a + '3')


a1 = SubAnimal1()
# a1.voice('animal_one')

a2 = SubAnimal2()
# a2.voice('animal_two')

a3 = SubAnimal3()
# a3.voice('animal_three')

Animal.get_num_ins()
