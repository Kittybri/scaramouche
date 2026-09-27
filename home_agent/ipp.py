"""Small fixed IPP subset: local CUPS test/print/status, never arbitrary documents."""

from __future__ import annotations
import struct
import secrets
from urllib.parse import quote
import aiohttp
from home.protocol import Rejected, NOTES


def attribute(tag, name, value):
    name = name.encode()
    if isinstance(value, str):
        value = value.encode()
    return (
        bytes([tag])
        + struct.pack(">H", len(name))
        + name
        + struct.pack(">H", len(value))
        + value
    )


def packet(operation, queue, template=None, job_id=None):
    rid = secrets.randbelow(2**30) + 1
    uri = "ipp://localhost/printers/" + quote(queue, safe="")
    data = struct.pack(">BBHI", 2, 0, operation, rid) + b"\x01"
    data += attribute(0x47, "attributes-charset", "utf-8") + attribute(
        0x48, "attributes-natural-language", "en"
    )
    data += attribute(0x45, "printer-uri", uri)
    if job_id is not None:
        data += attribute(0x21, "job-id", struct.pack(">i", job_id))
    if template is not None:
        if template not in NOTES:
            raise Rejected("unsupported_document")
        data += attribute(0x49, "document-format", "text/plain")
        data += attribute(0x22, "ipp-attribute-fidelity", b"\x01")
        data += b"\x02" + attribute(0x21, "copies", struct.pack(">i", 1))
        data += attribute(0x33, "page-ranges", struct.pack(">ii", 1, 1))
        data += attribute(0x44, "job-sheets", "none")
    return (
        data + b"\x03" + ((NOTES[template] + "\n").encode() if template else b""),
        rid,
    )


def parse(data, rid):
    if len(data) < 9 or len(data) > 65536:
        raise Rejected("invalid_ipp_response")
    _, _, status, response_id = struct.unpack(">BBHI", data[:8])
    if response_id != rid or status >= 0x0100:
        raise Rejected("print_failed")
    result = {}
    pos = 8
    while pos < len(data):
        tag = data[pos]
        pos += 1
        if tag == 3:
            break
        if tag < 0x10:
            continue
        if pos + 2 > len(data):
            raise Rejected("invalid_ipp_response")
        length = struct.unpack(">H", data[pos : pos + 2])[0]
        pos += 2
        name = data[pos : pos + length].decode("ascii", errors="replace")
        pos += length
        if pos + 2 > len(data):
            raise Rejected("invalid_ipp_response")
        size = struct.unpack(">H", data[pos : pos + 2])[0]
        pos += 2
        value = data[pos : pos + size]
        pos += size
        if len(value) != size:
            raise Rejected("invalid_ipp_response")
        if name in {"job-id", "job-state"} and size == 4:
            result[name] = struct.unpack(">i", value)[0]
    return result


class Printer:
    async def request(self, queue, operation, template=None, job_id=None):
        data, rid = packet(operation, queue, template, job_id)
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=5)
        ) as session:
            async with session.post(
                "http://127.0.0.1:631/printers/" + quote(queue, safe=""),
                data=data,
                headers={"Content-Type": "application/ipp"},
                allow_redirects=False,
            ) as response:
                if response.status != 200:
                    raise Rejected("printer_offline")
                body = await response.content.read(65537)
                return parse(body, rid)

    async def execute(self, d, action, p, media=None):
        import time

        if time.time() >= d.get("_expires_at", float("inf")):
            raise Rejected("expired_command")
        if action == "test":
            await self.request(d["queue"], 0x000B)
            return "completed"
        if action != "print_note":
            raise Rejected("unsupported_document")
        response = await self.request(d["queue"], 0x0002, p["template"])
        if response.get("job-id"):
            state = await self.request(d["queue"], 0x0009, job_id=response["job-id"])
            if state.get("job-state") == 9:
                return "completed"
            if state.get("job-state") in {7, 8}:
                raise Rejected("print_failed")
        return "submitted"

    async def close(self):
        pass
