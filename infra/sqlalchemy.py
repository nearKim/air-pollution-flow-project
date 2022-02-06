from geoalchemy2 import Geometry


class MysqlGeometry(Geometry):
    as_binary = "ST_AsWKB"
