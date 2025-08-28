import geopandas
import shapely


class Cuttings:

    def __init__(self, _instances: geopandas.GeoDataFrame):
        """

        _instances: the geometry field encodes the points from a single/distinct parent catchment
        """

        self.__instances = _instances

    def inside(self, x: shapely.geometry.polygon.Polygon):

        # Is y, a geometry point of self.__instances, a member of polygon x?
        outputs = self.__instances.geometry.apply(lambda y: y.within(x))

        return sum(outputs)
