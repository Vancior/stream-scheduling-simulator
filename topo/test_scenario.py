import os

import yaml
from yaml.loader import Loader

from topo.scenario import Scenario


def test_create_scenario_from_yaml():
    with open(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "../samples/a0.yaml"),
        "r",
    ) as f:
        data = yaml.load(f.read(), Loader=Loader)
        sc = Scenario.from_dict(data)
        print(sc.topo.get_nodes())
