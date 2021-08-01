from .coordinate import Coordinate, create_coordinate_class


def test_2d_random_unit_vector():
    coord_2d_class = create_coordinate_class(2)
    unit = coord_2d_class.random_unit_vector()
    print(unit)


def test_3d_random_unit_vector():
    coord_3d_class = create_coordinate_class(3)
    unit = coord_3d_class.random_unit_vector()
    print(unit)
