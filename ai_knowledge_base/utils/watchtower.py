import os
import logging
import redis
from sqlalchemy import create_engine, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session
from sqlalchemy.sql import and_
from typing import Any
import pandas as pd
import time 
import pickle

from ..model import DocumentIngestion, Document


class RDBMS:
    """
    Relational database system wrapper
    """

    def __init__(self, db=None, timeout=60):
        self.db = db
        self.timeout = timeout

    @classmethod
    def from_url(cls, url: str, **kwargs):
        """
        Initiate db management system from URL
        """
        try:
            db = create_engine(url, pool_pre_ping=True)
            return cls(
                db=db,
                timeout=kwargs.get("timeout", 60)
            )
        except ConnectionError:
            logging.warning(
                "RDBMS is not available, setup relational database to allow storing chat session history")
            return cls(
                db=None
            )


class CacheLocal(dict):

    def set(self, keyname, value, **kwargs):
        self.setdefault(keyname, value)


class CacheMS:
    """
    Cache management system wrapper
    """
    def __init__(self, cache, expire_time_second):
        self.expire_time_second = expire_time_second
        self.cache = cache

    @classmethod
    def from_url(cls, url: str, **kwargs):
        """
        Initiate cache management system from URL
        """
        try:
            cache = redis.Redis.from_url(url)
            cache.ping()
            logging.info("Cache is available through Redis server.")
            return cls(
                cache=cache,
                expire_time_second=kwargs.get("expire_time_second", 60)
            )
        except ConnectionError:
            logging.warning(
                "Cache server cannot be connected at the moment, try it later.")
            return cls(
                cache=CacheLocal(),
                expire_time_second=None
            )
        except:
            logging.warning(
                "Cache is not available, setup Redis cache to accelerate searching.")
            return cls(
                cache=CacheLocal(),
                expire_time_second=None
            )

    def set(self, key: str, value: Any, **kwargs):
        """
        Push general value to cache
        """
        self.cache.set(key, pickle.dumps(value), **kwargs)

    def get(self, key: str, default: Any, **kwargs):
        """
        Get general value from cache
        """
        value_serialized = self.cache.get(key, **kwargs)
        if value_serialized is None:
            value = default
        else:
            value = pickle.loads(value_serialized)
        return value
    

class IngestionWatchTower:
    """
    Manage the chat session history
    """

    def __init__(self, dbclient=None):
        self.rdbms = RDBMS.from_url(dbclient.db.url) if dbclient else None

    def query_document_ingestion(self, query):
        try:
        # if True:
            with Session(self.rdbms.db) as conn:
                ingestions = conn.execute(query).all()
            return ingestions
        except:
            logging.error(
                "RDBMS is not available, no ingestion history can be retrieved")
            return []

    def load_document_ingestion(self, document_ingestion):
        """
        Load document ingestion records from database
        """
        try:
            with Session(self.rdbms.db) as conn:
                query = select(DocumentIngestion).where(
                    and_(
                        DocumentIngestion.url == document_ingestion.url,
                        DocumentIngestion.checksum == document_ingestion.checksum
                    )
                )
                ingestion = conn.scalars(query).one_or_none()
            return ingestion
        except:
            logging.error(
                "RDBMS is not available, no ingestion history can be retrieved")
            return None

    def persist_document_ingestion(self, document_ingestion):
        """
        Persist document ingestion records to RDBMS
        """
        try:
            with Session(self.rdbms.db) as conn:
                query = insert(DocumentIngestion).values(
                    [document_ingestion.to_dict()]
                )
                query = query.on_conflict_do_update(
                    constraint="unique_document_ingestion_id",
                    set_=dict(
                        staging_path=query.excluded.staging_path,
                        size=query.excluded.size,
                        extraction_service_checksum=query.excluded.extraction_service_checksum,
                        structured_content=query.excluded.structured_content,
                        embedding_service_checksum=query.excluded.embedding_service_checksum,
                        embedding=query.excluded.embedding,
                        status=query.excluded.status,
                        error=query.excluded.error,
                        updated_dt=query.excluded.updated_dt,
                    )
                )
                conn.execute(query)
                conn.commit()
        except:
            logging.error(
                "RDBMS is not available, no ingestion history will be persist")