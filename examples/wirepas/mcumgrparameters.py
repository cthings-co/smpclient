"""Get the "MCUmgr" (AKA SMP) parameters."""

import argparse
import asyncio
from typing import Final

from smpclient import SMPClient
from smpclient.generics import error, success
from smpclient.requests.os_management import MCUMgrParametersRead
from smpclient.transport.wirepas import SMPWirepasTransport


async def main() -> None:
    parser = argparse.ArgumentParser(description="Echo 'Hello, World!' from an SMP server")
    parser.add_argument("address", type=int, help="The Wirepas Node address to connect to")
    address = parser.parse_args().address

    async with SMPClient(SMPWirepasTransport(), address) as client:
        print("OK")
        print(f"Client MTU is {client._transport.mtu}B")
        print(f"Client max unencoded size is {client._transport.max_unencoded_size}B")

        print("Sending request...", end="", flush=True)
        response: Final = await client.request(MCUMgrParametersRead())
        print("OK")

        if success(response):
            print(f"Received response: {response}")
        elif error(response):
            print(f"Received error: {response}")
        else:
            raise Exception(f"Unknown response: {response}")


if __name__ == "__main__":
    asyncio.run(main())
