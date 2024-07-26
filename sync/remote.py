import requests
import os
import time
from db import postgres
from datetime import datetime
from pony.orm import *
from ape.logging import logger

DYRPC_BASE_URL = os.getenv("DYRPC_BASE_URL", None)
DYRPC_TOKEN = os.getenv("DYRPC_TOKEN", None)

@db_session
def topup():
    if not DYRPC_BASE_URL:
        logger.error("please provide a DYRPC_BASE_URL")
        exit(1)
    if not DYRPC_TOKEN:
        logger.error("please provide a DYRPC_TOKEN")
        exit(1)

    topups = postgres.Topup.select(lambda t: t.processed == False)
    for t in topups:
        logger.info(f"uploading topup for tx_hash={t.tx_hash}, api_key_hash={t.api_key_hash}, amount_unit={t.amount_unit}")
        now = datetime.now()
        uploaded = post(t.api_key_hash, t.amount_unit)
        if uploaded:
            t.set(processed = True, updated_at = now)
            commit()
        else:
            logger.error(f"error uploding topup for tx_hash {t.tx_hash}: {t.api_key_hash}")

def post(api_key_hash: str, amount_unit: int) -> bool:
    url = f"{DYRPC_BASE_URL}/topupApiKey?apiKeyHash={api_key_hash}&amount={amount_unit}"
    res = requests.post(
      url,
      headers={"Authentication": f"Bearer {DYRPC_TOKEN}"}
    )
    return res.status_code == 200
