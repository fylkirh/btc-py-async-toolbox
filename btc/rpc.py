import random
from typing import List, Union

import aiohttp


class RPC:
    def __init__(self, method: str, params: list = []):
        self.method = method
        self.params = params
        self.jsonrpc = "2.0"
        self.id = random.randint(0, 2 ** 32)

    async def execute(self, url: str, session: aiohttp.ClientSession) -> Union[list, dict]:
        async with session.post(url, json=self._assemble()) as response:
            return await response.json()

    def _assemble(self):
        payload = {
            "method": self.method,
            "params": self.params,
            "jsonrpc": self.jsonrpc,
            "id": self.id,
        }
        return payload

    def __str__(self):
        return str(self._assemble())


class RPCBatch(RPC):
    def __init__(self, method_list: List[RPC]):
        self.rpc_list = method_list

    def _assemble(self):
        payload = [{
            "method": rpc.method,
            "params": rpc.params,
            "jsonrpc": rpc.jsonrpc,
            "id": rpc.id,
        } for rpc in self.rpc_list]
        return payload

    def __str__(self):
        return str(self._assemble())


class RPCException(BaseException):
    pass
