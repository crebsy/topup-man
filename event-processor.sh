#! /bin/bash
set -e

if [ "$CHAIN_ID" == "1" ]; then
  ape run events --network ethereum:mainnet
elif [ "$CHAIN_ID" == "8453" ]; then
  ape run events --network base:mainnet
fi
