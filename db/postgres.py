from pony.orm import *
from datetime import datetime
from ape.logging import logger
import os

db = Database()
db.bind(
    provider="postgres",
    user=os.getenv("POSTGRES_USER", "topup_man"),
    password=os.getenv("POSTGRES_PASSWORD", "topup_man"),
    host=os.getenv("POSTGRES_HOST", "localhost"),
    database=os.getenv("POSTGRES_DB", "topup_man")
)
#set_sql_debug(True)

class Topup(db.Entity):
    topup_id = PrimaryKey(int, auto=True)
    tx_hash = Required(str, index=True)
    block = Required(int, size=64, index=True)
    api_key_hash = Required(str, index=True)
    amount_dai = Required(str)
    amount_unit = Required(int, size=64)
    status = Required(str, index=True)
    created_at = Required(datetime)
    updated_at = Required(datetime)

db.generate_mapping(create_tables=True)

@db_session
def enqueue(tx_hash: str, block: int, api_key_hash: str, amount_dai: int):
    existing = Topup.get(tx_hash = tx_hash, api_key_hash = api_key_hash)
    if not existing:
        amount_dai_number = 0
        try:
            amount_dai_number = int(amount_dai, 16)
        except Exception as e:
            logger.error(e)
            logger.error(f"could not parse amount tx_hash {tx_hash}: {amount_dai}")
            return
        if amount_dai_number >= 0:
            amount_unit = calc_units(amount_dai_number)
            now = datetime.now()
            Topup(
                tx_hash = tx_hash,
                block = block,
                api_key_hash = api_key_hash,
                amount_dai = amount_dai,
                amount_unit = int(amount_unit),
                status = "new",
                created_at = now,
                updated_at = now
            )
            logger.info(f"adding topup event for tx_hash {tx_hash}")
            commit()


def calc_units(amount_dai_number: int) -> int:
    price_per_million = 0.15 * 10**18
    return 1_000_000 * amount_dai_number / price_per_million


@db_session
def get_last_processed_block() -> int:
    return max(t.block for t in Topup if t.status != "new")
