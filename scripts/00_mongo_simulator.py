import argparse
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List

from pymongo import MongoClient

LOGGER = logging.getLogger("mongo_simulator")
DATABASE_NAME = "ecommerce"
COLLECTION_NAME = "customers"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def base_seed_documents() -> List[Dict[str, Any]]:
    now = utc_now()
    return [
        {
            "customer_id": "C001",
            "full_name": "Alice Walker",
            "email": "alice.walker@example.com",
            "price": 125.50,
            "status": "active",
            "updated_at": now,
        },
        {
            "customer_id": "C002",
            "full_name": "Brian Hall",
            "email": "brian.hall@example.com",
            "price": 89.90,
            "status": "active",
            "updated_at": now,
        },
        {
            "customer_id": "C003",
            "full_name": "Carla Stone",
            "email": "carla.stone@example.com",
            "price": 199.00,
            "status": "active",
            "updated_at": now,
        },
    ]


def good_incremental_documents() -> List[Dict[str, Any]]:
    now = utc_now()
    return [
        {
            "customer_id": "C002",
            "full_name": "Brian Hall",
            "email": "brian.hall@example.com",
            "price": 109.90,
            "status": "active",
            "updated_at": now,
        },
        {
            "customer_id": "C004",
            "full_name": "Daniel Green",
            "email": "daniel.green@example.com",
            "price": 75.25,
            "status": "active",
            "updated_at": now,
        },
    ]


def bad_incremental_documents() -> List[Dict[str, Any]]:
    now = utc_now()
    return [
        {
            "customer_id": "C005",
            "full_name": "Evelyn Young",
            "email": "evelyn.young@example.com",
            "price": -50.00,
            "status": "active",
            "updated_at": now,
        },
        {
            "customer_id": "C006",
            "full_name": "Frank Black",
            "email": None,
            "price": 42.00,
            "status": "active",
            "updated_at": now,
        },
    ]


def get_collection(mongo_uri: str):
    client = MongoClient(mongo_uri)
    collection = client[DATABASE_NAME][COLLECTION_NAME]
    return client, collection


def reset_and_seed(mongo_uri: str) -> None:
    client, collection = get_collection(mongo_uri)
    try:
        LOGGER.info("Dropping collection %s.%s", DATABASE_NAME, COLLECTION_NAME)
        collection.drop()

        docs = base_seed_documents()
        result = collection.insert_many(docs)
        LOGGER.info("Inserted %d seed documents", len(result.inserted_ids))
    finally:
        client.close()


def add_good(mongo_uri: str) -> None:
    client, collection = get_collection(mongo_uri)
    try:
        docs = good_incremental_documents()
        result = collection.insert_many(docs)
        LOGGER.info("Inserted %d valid incremental documents", len(result.inserted_ids))
    finally:
        client.close()


def add_bad(mongo_uri: str) -> None:
    client, collection = get_collection(mongo_uri)
    try:
        docs = bad_incremental_documents()
        result = collection.insert_many(docs)
        LOGGER.info("Inserted %d anomalous incremental documents", len(result.inserted_ids))
    finally:
        client.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="MongoDB local simulator for the Databricks POC")
    parser.add_argument(
        "--mode",
        required=True,
        choices=["reset_and_seed", "add_good", "add_bad"],
        help="Simulation mode",
    )
    parser.add_argument(
        "--mongo-uri",
        default=os.getenv("MONGODB_URI", ""),
        help="MongoDB URI. Defaults to MONGODB_URI environment variable.",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    args = parse_args()
    if not args.mongo_uri:
        raise ValueError("MongoDB URI is required. Use --mongo-uri or set MONGODB_URI.")

    LOGGER.info("Running simulator mode: %s", args.mode)

    if args.mode == "reset_and_seed":
        reset_and_seed(args.mongo_uri)
    elif args.mode == "add_good":
        add_good(args.mongo_uri)
    elif args.mode == "add_bad":
        add_bad(args.mongo_uri)
    else:
        raise ValueError(f"Unsupported mode: {args.mode}")

    LOGGER.info("Simulator mode '%s' completed successfully", args.mode)


if __name__ == "__main__":
    main()
