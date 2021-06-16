import asyncio
import logging
import struct
from typing import Callable, Awaitable, Any

import zmq
import zmq.asyncio

from btc.utils import make_url


class ZMQHandler:
    def __init__(self, host: str, port: int,
                 hashblock_handler: Callable[[bytes], Awaitable[Any]] = None,
                 hashtx_handler: Callable[[bytes], Awaitable[Any]] = None,
                 rawblock_handler: Callable[[bytes], Awaitable[Any]] = None,
                 rawtx_handler: Callable[[bytes], Awaitable[Any]] = None,
                 sequence_handler: Callable[[bytes], Awaitable[Any]] = None):
        self.url = make_url(host, port=port, protocol="tcp")
        self.logger = logging.getLogger(__name__)

        self.hashtx_handler = hashtx_handler
        self.rawblock_handler = rawblock_handler
        self.rawtx_handler = rawtx_handler
        self.sequence_handler = sequence_handler
        self.loop = asyncio.get_event_loop()
        self.zmqContext = zmq.asyncio.Context()
        # noinspection PyUnresolvedReferences
        self.zmqSubSocket = self.zmqContext.socket(zmq.SUB)
        # noinspection PyUnresolvedReferences
        self.zmqSubSocket.setsockopt(zmq.RCVHWM, 0)
        if hashtx_handler:
            self.hashtx_handler = hashtx_handler
            # noinspection PyUnresolvedReferences
            self.zmqSubSocket.setsockopt_string(zmq.SUBSCRIBE, "hashtx")
        if rawblock_handler:
            self.rawblock_handler = rawblock_handler
            # noinspection PyUnresolvedReferences
            self.zmqSubSocket.setsockopt_string(zmq.SUBSCRIBE, "rawblock")
        if rawtx_handler:
            self.rawtx_handler = rawtx_handler
            # noinspection PyUnresolvedReferences
            self.zmqSubSocket.setsockopt_string(zmq.SUBSCRIBE, "rawtx")
        if sequence_handler:
            self.sequence_handler = sequence_handler
            # noinspection PyUnresolvedReferences
            self.zmqSubSocket.setsockopt_string(zmq.SUBSCRIBE, "sequence")
        if hashblock_handler:
            self.hashblock_handler = hashblock_handler
            # noinspection PyUnresolvedReferences
            self.zmqSubSocket.setsockopt_string(zmq.SUBSCRIBE, "hashblock")
        self.zmqSubSocket.connect(self.url)

    async def handle(self):
        topic, body, seq = await self.zmqSubSocket.recv_multipart()
        sequence = "Unknown"
        if len(seq) == 4:
            sequence = str(struct.unpack('<I', seq)[-1])
        if topic == b"hashblock":
            self.logger.debug('HASH BLOCK (' + sequence + '): ' + body.hex())
            await self.hashblock_handler(body)
        elif topic == b"hashtx":
            self.logger.debug('HASH TX  (' + sequence + '): ' + body.hex())
            await self.hashtx_handler(body)
        elif topic == b"rawblock":
            self.logger.debug('RAW BLOCK (' + sequence + '): ' + body.hex())
            await self.rawblock_handler(body)
        elif topic == b"rawtx":
            self.logger.debug('RAW TX (' + sequence + '): ' + body.hex())
            await self.rawtx_handler(body)
        elif topic == b"sequence":
            self.logger.debug('SEQUENCE (' + sequence + '): ' + body.hex())
            await self.sequence_handler(body)
        self.loop.create_task(self.handle())

    def start(self):
        self.loop.create_task(self.handle())

    def stop(self):
        self.zmqContext.destroy()
