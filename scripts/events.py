
import time
import os
from db import postgres
from ape import Contract, chain
from ape.logging import logger
from itertools import count

SAFE_ADDRESS = os.getenv("SAFE_ADDRESS", None)
if not SAFE_ADDRESS:
    logger.error("Please provide SAFE_ADDRESS")
    exit(1)

def main():
    DAI = get_contract("DAI_ADDRESS")
    USDC = get_contract("USDC_ADDRESS")
    WETH = get_contract("WETH_ADDRESS")

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
                for token in [DAI, USDC, WETH]:
                    process_events(token, token.symbol(), start_block, end_block)

                last_processed = end_block
        except Exception as e:
            raise e
        time.sleep(10)


def process_events(token: Contract, token_symbol: str, start_block: int, end_block: int):
    WETH_FEED = get_contract("WETH_FEED_ADDRESS")
    print(token)
    logger.info(f"token={token_symbol}, start_block={start_block}, end_block={end_block}, diff={end_block-start_block+1}")

    topics = { "to": SAFE_ADDRESS }
    if chain.chain_id == 1:
        if token_symbol == "WETH" or token_symbol == "DAI":
            topics = { "dst": SAFE_ADDRESS }
    elif chain.chain_id == 8453:
        if token_symbol == "WETH":
            topics = { "dst": SAFE_ADDRESS }

    events = token.Transfer.range(
        start_block,
        end_block+1,
        topics
    )

    rate = 1.0
    if token_symbol == "WETH":
        rate = WETH_FEED.latestAnswer() / 10**WETH_FEED.decimals()

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
        amount = call_data[len(call_data)-128:-64]

        if len(api_key_hash) != 64:
           logger.error(f"found invalid api_key_hash for tx {e.transaction_hash}: {api_key_hash}")
           continue

        if len(amount) != 64:
           logger.error(f"found invalid amount for tx {e.transaction_hash}: {amount}")
           continue

        postgres.enqueue(
          e.transaction_hash,
          e.block_number,
          api_key_hash,
          amount,
          token_symbol,
          rate
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

def get_contract(env_name: str):
    val = os.getenv(env_name, None)
    if not val:
        logger.error(f"Please provide {env_name}!")
        exit(1)

    return Contract(val)
