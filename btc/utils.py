import operator
from functools import reduce
from typing import List


def make_url(host: str, *, user: str = None, password: str = None, port: int = None, protocol: str = None):
    res = ""
    if protocol:
        res += protocol + "://"
    if user:
        res += user
        if password:
            res += ":" + password
        res += "@"
    res += host
    if port:
        res += ":" + str(port)
    return res


def split_into_chunks(lst: list, chunk_size):
    chunk_size = max(chunk_size, 1)
    return (lst[i: i + chunk_size] for i in range(0, len(lst), chunk_size))


def flatten(lst: List[list]) -> list:
    return reduce(operator.concat, lst)


def crop_utxo_to_input_format(utxo: dict) -> dict:
    return {"txid": utxo["txid"], "vout": utxo["vout"]}
