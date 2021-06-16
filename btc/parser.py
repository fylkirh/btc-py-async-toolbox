from typing import List, Tuple


class BaseParser:
    def __init__(self, payload: dict):
        self.json = payload
        self.verify()

    def verify(self):
        pass


class GetBlockParser(BaseParser):

    def verify(self):
        # verify verbosity
        if "tx" in self.json:
            if 'txid' in self.json['tx'][0]:
                return
        raise ParserException("Only verbosity=2 supported")

    def get_unique_output_addresses(self) -> List[str]:
        scriptpubkeys = [vout['scriptPubKey'] for tx in self.json['tx'] for vout in tx['vout']]
        res = set()
        for spk in scriptpubkeys:
            if 'addresses' in spk:
                res.add(spk['addresses'][0])
        return list(res)

    def get_coinbase_addresses(self) -> List[str]:
        scriptpubkeys = [vout['scriptPubKey'] for vout in self.json['tx'][0]['vout']]
        res = set()
        for spk in scriptpubkeys:
            if 'addresses' in spk:
                res.add(spk['addresses'][0])
        return list(res)

    def get_height(self) -> int:
        return int(self.json["height"])

    def get_transaction_ids(self) -> list:
        return [tx['txid'] for tx in self.json['tx']]


class ListUnspentParser(BaseParser):

    def get_addresses_and_amounts(self) -> Tuple[List[str], List[float]]:
        addr_amount = {}
        for elem in self.json:
            addr = elem["address"]
            amount = elem["amount"]
            if addr not in addr_amount:
                addr_amount[addr] = amount
            else:
                addr_amount[addr] += amount

        addrs = list(addr_amount.keys())
        amounts = list(addr_amount.values())
        return addrs, amounts


class ParserException(BaseException):
    pass
