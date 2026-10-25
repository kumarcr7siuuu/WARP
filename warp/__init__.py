"""
WARP: Wireless Activation Ring Protocol
High-Speed User-Space Reliability Layer over Custom UDP.
"""

from .protocol import (
    WarpHeader,
    WarpPacket,
    PacketType,
    FLAG_IS_NACK,
    FLAG_IS_PIGGYBACK,
    FLAG_END_OF_BATCH,
    FLAG_FIN,
    FLAG_SYN_CTRL,
    FLAG_COMPRESSED,
    FLAG_HEARTBEAT,
    FLAG_RANGE_NACK,
    HEADER_SIZE,
    DEFAULT_PAYLOAD_SIZE,
    DEFAULT_APP_ID,
)
from .buffer import SenderWindow, ReceiverSlottedBuffer
from .sender import WarpSender
from .receiver import WarpReceiver

__version__ = "1.0.0"
__all__ = [
    "WarpHeader",
    "WarpPacket",
    "PacketType",
    "FLAG_IS_NACK",
    "FLAG_IS_PIGGYBACK",
    "FLAG_END_OF_BATCH",
    "FLAG_FIN",
    "FLAG_SYN_CTRL",
    "FLAG_COMPRESSED",
    "FLAG_HEARTBEAT",
    "FLAG_RANGE_NACK",
    "HEADER_SIZE",
    "DEFAULT_PAYLOAD_SIZE",
    "DEFAULT_APP_ID",
    "SenderWindow",
    "ReceiverSlottedBuffer",
    "WarpSender",
    "WarpReceiver",
]
