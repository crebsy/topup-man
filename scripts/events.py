
import time
import os
from db import postgres
from ape import Contract, chain
from ape.logging import logger
from itertools import count

DAI = Contract("0x50c5725949A6F0c72E6C4a641F24049A917DB0Cb")
SAFE_ADDRESS = os.getenv("SAFE_ADDRESS", None)
if not SAFE_ADDRESS:
    logger.error("please provide a SAFE_ADDRESS")#
    exit(1)

SAFE = Contract(SAFE_ADDRESS)

def main():
    last_processed = postgres.get_last_processed_block()
    if not last_processed:
        last_processed = os.getenv("START_BLOCK", None)
        if not last_processed:
            #17518780
            logger.error("please provide a START_BLOCK")
            exit(1)
        last_processed = int(last_processed)
    while True:
        try:
            log_range_size = 1000
            for r in get_ranges(last_processed, int(log_range_size)):
                start_block = r[0]
                end_block = r[1]
                process_events(start_block, end_block)
                last_processed = end_block
        except Exception as e:
            raise e
        time.sleep(10)


def process_events(start_block: int, end_block: int):
    logger.info(f"start_block={start_block}, end_block={end_block}, diff={end_block-start_block+1}")
    events = DAI.Transfer.range(
        start_block,
        end_block+1,
        { "to": SAFE }
    )
    for e in events:
        tx = chain.provider._make_request(
            "eth_getTransactionByHash",
            [e.transaction_hash]
        )
        call_data = tx["input"]
        if len(call_data) != 202:
            logger.error(f"found invalid call_data for tx {e.transaction_hash}: {call_data}")
            continue

        api_key_hash = call_data[len(call_data)-64:]
        amount_dai = call_data[len(call_data)-128:-64]

        if len(api_key_hash) != 64:
           logger.error(f"found invalid api_key_hash for tx {e.transaction_hash}: {api_key_hash}")
           continue

        if len(api_key_hash) != 64:
           logger.error(f"found invalid amount_dai for tx {e.transaction_hash}: {amount_dai}")
           continue

        postgres.enqueue(
          e.transaction_hash,
          e.block_number,
          api_key_hash,
          amount_dai
        )


def get_ranges(start_block: int, steps: int):
    head = chain.blocks.head.number
    end = 0
    ranges = []
    for i in count():
        if end > head:
            break

        start = start_block + (i * steps)
        end = start_block + ((i+1) * steps)-1
        to = min(end, head)
        #logger.debug("[%d, %d]" % (min(start, to), to))
        ranges.append([min(start, to), to])

    return ranges