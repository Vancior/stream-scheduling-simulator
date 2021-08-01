import math
import random


class Coordinate:
    dim = 2

    def __init__(self, *args) -> None:
        assert len(args) == self.dim
        self.co = args

    def __str__(self) -> str:
        return "({})".format(",".join([str(i) for i in self.co]))

    def __add__(self, c):
        assert c.dim == self.dim
        return self.__class__(*[i + j for i, j in zip(self.co, c.co)])

    def __sub__(self, c):
        assert c.dim == self.dim
        return self.__class__(*[i - j for i, j in zip(self.co, c.co)])

    def __abs__(self):
        return math.sqrt(sum([i * i for i in self.co]))

    def __mul__(self, scalar: float):
        return self.__class__(*[i * scalar for i in self.co])

    def __truediv__(self, scalar: float):
        return self.__class__(*[i / scalar for i in self.co])

    @classmethod
    def zero(cls):
        return cls(*[0 for _ in range(cls.dim)])

    @classmethod
    def random_unit_vector(cls):
        assert cls.dim > 0
        co = [0 for i in range(cls.dim)]
        if cls.dim == 1:
            co[0] = random.randint(0, 1) * 2 - 1
            return cls(*co)

        co[0] = 1
        for i in range(1, cls.dim):
            rad = random.random() * math.pi * 2
            for j in range(0, i):
                co[j] *= math.cos(rad)
            co[i] = math.sin(rad)
        return cls(*co)


def create_coordinate_class(dim: int):
    return type(str(dim) + "DCoordinate", (Coordinate,), {"dim": dim})
