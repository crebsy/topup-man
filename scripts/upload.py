from sync import remote
from ape.logging import logger
import time

def main():
    while True:
        try:
            remote.topup()
            time.sleep(1)
        except Exception as e:
            logger.error(e)
