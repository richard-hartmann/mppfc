import inspect

class Base:
    def __new__(cls, *args, **kwargs):
        print(f'exec Base.__new__(cls={cls}, args={args}, kwargs={kwargs})')
        return object.__new__(cls)

    def __init__(self, value):
        print(f'exec Base.__init__(value={value})')
        self.base_value = value
        try:
            # multi inheritance needs this, since for C(Base, SecondBase)
            # super().__init__(value) resolves to Base.__init__(value)
            # and the super inside Base.__init__(value), i.a. right here,
            # resolves to SecondBase.__init__(value)
            # On the other hand, when instantiating Base directly
            # super resolves to object.__init__() which does not take any arguments
            super().__init__(value)
        except:
            super().__init__()

    def __init_subclass__(cls, **kwargs):
        print(f'exec Base.__init_subclass__(cls={cls}, kwargs={kwargs})')
        print(f"MRO of {cls}: {cls.__mro__}")
        try:
            super().__init_subclass__(**kwargs)
        except Exception as e:
            print(e)

    def show_base(self):
        print(f"from Base: base_value={self.base_value}")

class SecondBase:
    def __new__(cls, *args, **kwargs):
        print(f'exec SecondBase.__new__(cls={cls}, args={args}, kwargs={kwargs})')
        return object.__new__(cls)

    def __init__(self, value):
        print(f'exec SecondBase.__init__(base_value={value})')
        self.snd_base_value = value

    def __init_subclass__(cls, **kwargs):
        print(f'exec SecondBase.__init_subclass__(cls={cls}, kwargs={kwargs})')
        print(f"MRO of {cls}: {cls.__mro__}")
        # this allows super() to propagate all the way along the method resolution order
        # calling __init_subclass__ until object is reached, which does not accept the kwargs argument
        try:
            super().__init_subclass__(**kwargs)
        except Exception as e:
            print(e)

    def show_snd_base(self):
        print(f"from SecondBase: base_value={self.snd_base_value}")


# the keyword-argument gets passed to __init_subclass__
class A(Base):
    def __init__(self, a_value, base_value='base'):
        print(f'exec A.__init__(a_value={a_value}, base_value={base_value})')
        super().__init__(value=base_value)
        self.a_value = a_value


class C(Base, SecondBase, msg="hi"):
    def __init__(self):
        print(f'exec C.__init__()')
        print(super().__init__)
        print(super().__init__.__qualname__)
        print(super().__init__.__name__)

        super().__init__(value=2)


class D(A):
    def __init__(self):
        print("exec D.__init__")
        super().__init__(a_value=12, base_value='14')

# stuff gets logged while setting up the class definitions
# no instantiation yet!
# __init_subclass__ gets called when DEFINING a subclass of Base,
# the class of the sub_class is passed, and additional keyword arguments are possible
print("*"*32)

#
#   init the base class
#

b = Base(4)
# __new__ with cls=Base and args=(4,), that's easy
print("*"*32)

#
#   init A, i.e., a subclass of base class
#

a = A(a_value=3)
# __new__ of base class gets called with kwargs 'a_value'=3 and NO base_value
# that combination would not allow to instantiate the base class!
print("*"*32)

# so let's check what happens ...
try:
    Base(a_value=3)
except TypeError as e:
    print(e)
# indeed, Base.__new__ gets executed, but __init__ fails!

# TODO: WE LEARN: __new__ gets exactly the arguments that trigger the class instantiation
print("*"*32)

c = C()
c.show_base()
c.show_snd_base()


class C0a:
    def __init_subclass__(cls, **kwargs):
        cls.inheritance += "c0a - "
        if inspect.isbuiltin(super().__init_subclass__):
            print("C0a: call object.__init_subclass__")
        else:
            print("C0a: call", super().__init_subclass__.__qualname__)
        super().__init_subclass__(**kwargs)

class C0b:
    def __init_subclass__(cls, **kwargs):
        cls.inheritance += "c0b - "
        if inspect.isbuiltin(super().__init_subclass__):
            print("C0b: call object.__init_subclass__")
        else:
            print("C0b: call", super().__init_subclass__.__qualname__)
        super().__init_subclass__(**kwargs)


print("setup C1")
class C1(C0a, C0b):
    inheritance = ""
    def __init_subclass__(cls, **kwargs):
        cls.inheritance += "c1 - "
        if inspect.isbuiltin(super().__init_subclass__):
            print("C01: call object.__init_subclass__")
        else:
            print("C01: call", super().__init_subclass__.__qualname__)
        super().__init_subclass__(**kwargs)

print(C1.__mro__)

print("setup C2")
class C2(C1):
    inheritance = ""

c = C2
print(c.inheritance)
print(c.mro())
