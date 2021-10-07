import argparse
import random
import typing


def gen_dag(args: argparse.Namespace):
    total_ranks = args.total_ranks
    max_per_rank = args.max_per_rank
    max_predecessors = args.max_predecessors
    rank_nodes = [["source"]]
    for rank in range(1, total_ranks):
        cur_node_cnt = random.randint(1, max_per_rank)
        cur_nodes = ["{}-{}".format(rank, i) for i in range(cur_node_cnt)]
        for node in cur_nodes:
            pre_cnt = random.randint(1, max_predecessors)
            for pre_node in select_predecessors(rank_nodes, pre_cnt, 0.5):
                print("{} ---> {}".format(pre_node, node))
        rank_nodes.append(cur_nodes)


def select_predecessors(
    rank_nodes: typing.List[typing.List[str]], cnt: int, drop_rate: float
):
    choices = []
    quota = cnt
    for lv in range(len(rank_nodes) - 1, -1, -1):
        sample_cnt = min(random.randint(0, quota), len(rank_nodes[lv]))
        quota -= sample_cnt
        for sample in random.sample(rank_nodes[lv], sample_cnt):
            choices.append(sample)
        if quota == 0:
            break
    if len(choices) == 0:
        choices.append(rank_nodes[0][0])
    return choices


def parse_args():
    parser = argparse.ArgumentParser(prog="gen.py")
    parser.add_argument("total_ranks", type=int)
    parser.add_argument("max_per_rank", type=int)
    parser.add_argument("max_predecessors", type=int)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    gen_dag(args)
