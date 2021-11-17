import asyncio
import logging
from typing import Optional, Tuple, List, Union, Dict

import aiohttp

from btc.rpc import RPC, RPCBatch
from btc.utils import make_url, split_into_chunks, flatten, crop_utxo_to_input_format

CHUNK_SIZE = 100


def process_rpc_response(response: Dict, return_error: bool) -> Optional[Union[str, Dict, Tuple[Dict, Dict]]]:
    if not response:
        return None
    if return_error:
        return response['result'], response['error']
    else:
        return response['result']


def process_rpc_response_batch(response: List[Dict], return_error: bool) -> Union[List[str], List[Dict],
                                                                                  List[Tuple[Dict, Dict]]]:
    if not response:
        return []
    if return_error:
        return [(r['result'], r['error']) for r in response]
    else:
        return [r['result'] for r in response]


class RPCHandler:
    def __init__(self, user: str, password: str, host: str, port: int):
        self.logger = logging.getLogger(__name__)
        self.url = make_url(host, user=user, password=password, port=port, protocol="http")
        self.loop = asyncio.get_event_loop()
        self.connected = False
        self.session = None
        self.timeout = aiohttp.ClientTimeout(total=1200)

    def __del__(self):
        if not self.session.closed:
            asyncio.create_task(self.session.close())

    def is_connected(self) -> bool:
        self.loop.run_until_complete(self._test_connection())
        return self.connected

    async def _init_session(self):
        if not self.session:
            self.session = aiohttp.ClientSession(loop=self.loop, timeout=self.timeout)

    async def _test_connection(self):
        await self._init_session()
        test_rpc = RPC("getbalance")
        try:
            await test_rpc.execute(self.url, self.session)
            self.logger.debug("Connected to Bitcoin node at " + self.url)
            self.connected = True
        except Exception as e:
            self.logger.error("Can't connect to Bitcoin node. " + str(e))
            self.connected = False

    async def _execute_rpc_single(self, method: str, params=None) -> Optional[dict]:
        await self._init_session()
        if params is None:
            params = []
        rpc = RPC(method, params)
        try:
            self.logger.debug("Executing RPC: " + str(rpc))
            result = await rpc.execute(self.url, self.session)

            if result['error'] is not None:
                self.logger.debug(f"Executed RPC with error: {str(rpc)}: {result['error']}")
            else:
                self.logger.debug(f"Executed RPC: {str(rpc)}. Result: {result['result']}")
            return result
        except Exception as e:
            self.logger.exception("Can't execute RPC: " + str(rpc) + " " + str(e))
            return None

    async def _execute_rpc_batch(self, method_list: List[Tuple[str, list]]) -> Optional[List[Dict]]:
        if not method_list:
            return []
        await self._init_session()
        chunks = split_into_chunks(method_list, CHUNK_SIZE)
        rpc_batch_chunks = [RPCBatch([RPC(method, params) for method, params in c]) for c in chunks]
        try:
            self.logger.debug("Executing RPC batch: " + str([str(rpc) for rpc in rpc_batch_chunks]))
            res = []
            # Has to be done sequentially instead of asyncio.gather() to prevent asyncio timeouts on large requests.
            # Most of RPC requests are handled by one thread anyway, so practically there is no performance drop from
            # this approach. Also, it keeps the node's RPC queue shorter, allowing better access for other services.
            for rpc in rpc_batch_chunks:
                res.append(await rpc.execute(self.url, self.session))
            self.logger.debug("Executed RPC batch: " + str([str(rpc) for rpc in rpc_batch_chunks]))
            if res:
                # noinspection PyTypeChecker
                return flatten(res)
            else:
                return []
        except Exception as e:
            self.logger.exception("Can't execute RPC batch: " + str(e))
            return None

    async def get_block(self, blk_hash: str, return_error: bool = False):
        resp = await self._execute_rpc_single("getblock", [blk_hash, 2])
        return process_rpc_response(resp, return_error)

    async def get_block_hash(self, blk_height: int, return_error: bool = False):
        resp = await self._execute_rpc_single("getblockhash", [blk_height])
        return process_rpc_response(resp, return_error)

    async def get_block_hash_batch(self, blk_height_list: List[int], return_error: bool = False):
        method_list = [("getblockhash", [blk_height]) for blk_height in blk_height_list]
        resp = await self._execute_rpc_batch(method_list)
        return process_rpc_response_batch(resp, return_error)

    async def get_block_batch(self, blk_hash_list: List[str], return_error: bool = False) -> list:
        method_list = [("getblock", [blk_hash, 2]) for blk_hash in blk_hash_list]
        resp = await self._execute_rpc_batch(method_list)
        return process_rpc_response_batch(resp, return_error)

    async def get_new_address(self, return_error: bool = False):
        resp = await self._execute_rpc_single("getnewaddress")
        return process_rpc_response(resp, return_error)

    async def get_new_address_batch(self, number: int, return_error: bool = False):
        method_list = [("getnewaddress", []) for _ in range(number)]
        resp = await self._execute_rpc_batch(method_list)
        return process_rpc_response_batch(resp, return_error)

    async def get_received_by_address(self, address: str, return_error: bool = False):
        resp = await self._execute_rpc_single("getreceivedbyaddress", [address])
        return process_rpc_response(resp, return_error)

    async def get_received_by_address_batch(self, address_list: List[str], return_error: bool = False):
        method_list = [("getreceivedbyaddress", [address]) for address in address_list]
        resp = await self._execute_rpc_batch(method_list)
        return process_rpc_response_batch(resp, return_error)

    async def list_unspent(self, addresses: List[str] = None, minconf: int = 1, maxconf: int = 999999,
                           return_error: bool = False):
        if addresses is None:
            addresses = []
        resp = await self._execute_rpc_single("listunspent", [minconf, maxconf, addresses])
        return process_rpc_response(resp, return_error)

    async def list_received_by_address(self, return_error: bool = False):
        resp = await self._execute_rpc_single("listreceivedbyaddress", [])
        return process_rpc_response(resp, return_error)

    async def create_raw_transaction(self, inputs: List[dict], outputs: dict, return_error: bool = False):
        inputs_cropped = list(map(crop_utxo_to_input_format, inputs))
        resp = await self._execute_rpc_single("createrawtransaction", [inputs_cropped, outputs])
        return process_rpc_response(resp, return_error)

    async def sign_raw_transaction(self, raw_tx: str, return_error: bool = False):
        resp = await self._execute_rpc_single("signrawtransactionwithwallet", [raw_tx])
        return process_rpc_response(resp, return_error)

    async def send_raw_transaction(self, raw_tx: str, return_error: bool = False):
        resp = await self._execute_rpc_single("sendrawtransaction", [raw_tx])
        return process_rpc_response(resp, return_error)

    async def estimate_smart_fee(self, target: int, return_error: bool = False):
        resp = await self._execute_rpc_single("estimatesmartfee", [target])
        return process_rpc_response(resp, return_error)

    async def get_balance(self, return_error: bool = False):
        resp = await self._execute_rpc_single("getbalance", [])
        return process_rpc_response(resp, return_error)

    async def list_transactions(self, count: int = 10, return_error: bool = False):
        resp = await self._execute_rpc_single("listtransactions", ["*", count])
        return process_rpc_response(resp, return_error)

    async def send_to_address(self, addr: str, amount: float, return_error: bool = False):
        resp = await self._execute_rpc_single("sendtoaddress", [addr, amount])
        return process_rpc_response(resp, return_error)

    async def get_block_count(self, return_error: bool = False):
        resp = await self._execute_rpc_single("getblockcount", [])
        return process_rpc_response(resp, return_error)

    async def get_transaction(self, txid: str, return_error: bool = False):
        resp = await self._execute_rpc_single("gettransaction", [txid])
        return process_rpc_response(resp, return_error)

    async def get_transaction_batch(self, txid_list: List[str], return_error: bool = False):
        method_list = [("gettransaction", [txid]) for txid in txid_list]
        resp = await self._execute_rpc_batch(method_list)
        return process_rpc_response_batch(resp, return_error)

    async def list_since_block(self, blk_hash: str, return_error: bool = False):
        resp = await self._execute_rpc_single("listsinceblock", [blk_hash])
        return process_rpc_response(resp, return_error)

    async def get_address_info(self, address: str, return_error: bool = False):
        resp = await self._execute_rpc_single("getaddressinfo", [address])
        return process_rpc_response(resp, return_error)

    async def get_raw_mempool(self, return_error: bool = False):
        resp = await self._execute_rpc_single("getrawmempool", [])
        return process_rpc_response(resp, return_error)

    async def get_block_by_height(self, blk_height: int):
        blk_hash = await self.get_block_hash(blk_height)
        return await self.get_block(blk_hash)
