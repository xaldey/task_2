import asyncio
from concurrent.futures import ThreadPoolExecutor
from enum import Enum
from dataclasses import dataclass
from datetime import datetime
from typing import List
import random


class Result(Enum):
    Accepted = 1
    Rejected = 2


@dataclass
class Address:
    name: str
    email: str


@dataclass
class Payload:
    content: str


@dataclass
class Event:
    recipients: List[Address]
    payload: Payload


async def read_data() -> Event:
    recipients = [
        Address("Wallie", "wallie@mail.ru"),
        Address("Eva", "eva@gmail.com"),
    ]
    payload = Payload("Let's make it to work easy to understand")
    return Event(recipients, payload)


async def send_data(dest: Address, payload: Payload) -> Result:
    status = random.choice([Result.Accepted, Result.Rejected])
    return status


async def perform_operation():
    READ_DELAY = 1
    RETRY_DELAY = 5

    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor() as pool:
        while True:
            event = await read_data()
            for recipient in event.recipients:
                result = await loop.run_in_executor(
                    pool, send_data, recipient, event.payload
                )
                if result is Result.Rejected:
                    await asyncio.sleep(RETRY_DELAY)
                    result = await loop.run_in_executor(
                        pool, send_data, recipient, event.payload
                    )
                    if result is Result.Rejected:
                        print(f"Не удалось направить данные {recipient.name}")
                    else:
                        print(f"Данные были успешно отправлены {recipient.name}")
                else:
                    print(f"Данные были успешно отправлены {recipient.name}")
            await asyncio.sleep(READ_DELAY)


asyncio.run(perform_operation())
