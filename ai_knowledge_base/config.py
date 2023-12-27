import os
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
    index_store: Connection = Connection(url="mongodb+srv://username:password@akb-cosmosdb-mongo.mongocluster.cosmos.azure.com/?tls=true&authMechanism=SCRAM-SHA-256&retrywrites=false&maxIdleTimeMS=120000")


@dataclass_json
@dataclass
class EmbeddingConfig:
    """
    Embedding configuration
    """
    data_path: str
    connection_string: str
    database_name: str
    collection_name: str
    index_name: str
    vector_field: str
    language: Optional[str] = None
    chunk_size: Optional[int] = 1024
    token_overlap: Optional[int] = 128


@dataclass_json
@dataclass
class PromptExample:
    """
    Prompt example
    """
    input: str
    output: str


@dataclass_json
@dataclass
class StructuredPrompt:
    """
    Prompt configuration.
    """
    context: Optional[str] = None
    examples: Optional[List[PromptExample]] = field(default_factory=list)
    input_template: Optional[str] = None
    web_search_template: Optional[str] = None
    proprietary_search_template: Optional[str] = None


@dataclass_json
@dataclass
class ModelConfig:
    """
    Model input
    """

    model_id: str
    tag: Optional[str] = None
    prompt: Optional[StructuredPrompt] = None
    parameters: Optional[dict] = field(default_factory=dict)
