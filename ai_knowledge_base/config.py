import os
import logging
import hashlib
from typing import Optional, Dict, List, Any
from dataclasses import dataclass, field
from dataclasses_json import dataclass_json


@dataclass_json
@dataclass
class Connection:
    """
    Data source connection.

    :param host: data source/server host. Optional if host is provided through URL.
    :type host: str
    :param username: username credential. Optional if credential are provided through URL.
    :type username: str
    :param password: password credential. Optional if credential are provided through URL.
    :type password: str
    :param url: data source/server connection string. Optional if host, username, and password are provided.
    :type url: str
    :param expire_time_second: time for data to expire, unit in seconds. Applicable to cache only.
    :type expire_time_second: int

    """

    host: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    url: Optional[str] = None
    expire_time_second: Optional[int] = 60


@dataclass_json
@dataclass
class DBClient:
    """
    Client to connect with data sources

    :param rdbms: connection detail for RDBMS.
    :type rdbms: sre.config.Connection
    :param redis: connection detail for Redis Cache.
    :type redis: sre.config.Connection

    """

    db: Connection = Connection(
        url="postgresql+psycopg2://username:password@host:port/database"
    )
    cache: Connection = Connection(url="redis://@localhost:6377/0")


@dataclass_json
@dataclass
class Service:

    type: str
    endpoint: str
    secret: Optional[str] = None
    specs: Optional[dict] = field(default_factory=dict)

    @property
    def checksum(self):
        return hashlib.md5(" ".join([self.type, self.endpoint, str(self.specs)]).encode()).hexdigest()

    def __post_init__(self):
        if self.type == "openai_embedding" and not ("deployment" in self.specs or "api_version" in self.specs):
            raise Exception("ERROR: deployment name and api_version are required by OpenAI Embedding service. Please provide these values.")
        if self.type == "azure_doc_intelligence" and not ("model_type" in self.specs):
            raise Exception("ERROR: model_type is required by Azure Doc Intelligence service. Please provide these values.")


@dataclass_json
@dataclass
class IndexStore: 
    """
    Index store for AI to retrieve embeddings
    """
    index_name: str
    index_service: Service
    semantic_config_name: str = "semantic_default"
    vector_config_name: Optional[str] = None
    embedding_service: Optional[Service] = None
    doc_extract_type: Optional[str] = "OCR"
    doc_extract_service: Optional[Service] = None

    def __post_init__(self):
        if self.vector_config_name and not self.embedding_service:
            raise Exception("ERROR: Vector search is enabled in the config, but no embedding model endpoint and key were provided. Please provide these values or disable vector search.")
        if self.doc_extract_type in ["DOC_ANALYSIS","OCR"] and (not self.doc_extract_service):
            raise Exception("ERROR: Document extraction requires Doc Intelligence service, but no service endpoint and key were provided. Please provide these values.")

@dataclass_json
@dataclass
class RetrievalMethod:
    """
    Retrieval methodology
    """
    type: str = "WEB_SEARCH" # PROPRIETARY_SEARCH, WEB_SEARCH, ARVIX_SEARCH
    index_store: Optional[IndexStore] = None

    def __post_init__(self):
        if not self.type in ["PROPRIETARY_SEARCH", "WEB_SEARCH", "ARVIX_SEARCH"]:
            raise Exception(f"ERROR: retrieval type {self.type} is not yet supported. Please specify one of the following: [BINARY, DOC_ANALYSIS, OCR].")
        if self.type == "PROPRIETARY_SEARCH" and (not self.index_store):
            raise Exception("ERROR: Propiertary search requires an index store to retrieve internal information, but no index store endpoint and key were provided. Please provide these values.")

@dataclass_json
@dataclass
class IngestionConfig:
    """
    Data ingestion configuration
    """
    data_path: str
    staging_path: str
    retrieval_method: RetrievalMethod
    database: Optional[DBClient] = None
    url_prefix: Optional[str] = None
    language: Optional[str] = None
    chunk_size: Optional[int] = 1024
    token_overlap: Optional[int] = 128
