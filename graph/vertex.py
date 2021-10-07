class Vertex:
    uuid: str

    def __init__(self, uuid, data) -> None:
        self.uuid = uuid
        self.data = data

    def __str__(self) -> str:
        return "Vertex[{}] {}".format(self.uuid, str(self.data))

    def __repr__(self) -> str:
        return self.__str__()

    @classmethod
    def from_spec(
        cls,
        uuid: str,
        type: str,
        domain_constraint: dict,
        out_unit_size: int,  # in bytes
        out_unit_rate: float,  # per second
        mi: int,
        memory: int,
    ):
        data = {
            "type": type,
            "domain_constraint": domain_constraint,
            "out_unit_size": out_unit_size,
            "out_unit_rate": out_unit_rate,
            "mi": mi,
            "memory": memory,
            "upstream_bd": 0,
            "downstream_bd": 0,
        }
        return cls(uuid, data)

    @classmethod
    def from_networkx(cls, uuid: str, data: dict):
        return cls(uuid, data)

    @property
    def type(self) -> str:
        return self.data["type"]

    @property
    def domain_constraint(self) -> dict:
        return self.data["domain_constraint"]

    @property
    def out_unit_size(self) -> int:
        return self.data["out_unit_size"]

    @property
    def out_unit_rate(self) -> int:
        return self.data["out_unit_rate"]

    @property
    def mi(self) -> int:
        return self.data["mi"]

    @property
    def memory(self) -> int:
        return self.data["memory"]

    @property
    def upstream_bd(self) -> int:
        return self.data["upstream_bd"]

    @property
    def downstream_bd(self) -> int:
        return self.data["downstream_bd"]
