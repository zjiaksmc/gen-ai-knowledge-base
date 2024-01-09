import os
from datetime import datetime
from typing import List, Optional, Any, Dict
from dataclasses import dataclass, field, fields
import hashlib
from dataclasses_json.cfg import config
from dataclasses_json import dataclass_json
from sqlalchemy import String, MetaData, Table, Column, Integer, Identity
from sqlalchemy.orm import registry

mapper_registry = registry()
metadata_obj = MetaData()


@dataclass_json
@dataclass
class Document(object):
    """A data class for storing documents

    Attributes:
        id (Optional[str]): The id of the document.
        title (Optional[str]): The title of the document.
        content (Optional[str]): The content of the document.
        filepath (Optional[str]): The filepath of the document.
        url (Optional[str]): The url of the document.
        metadata (Optional[Dict]): The metadata of the document.    
    """
    id: Optional[str] = None
    title: Optional[str] = None
    content: Optional[str] = None
    filepath: Optional[str] = None
    url: Optional[str] = None
    metadata: Optional[Dict] = None
    contentVector: Optional[List[float]] = None


@dataclass_json
@dataclass
class ChunkingResult:
    """Data model for chunking result

    Attributes:
        chunks (List[Document]): List of chunks.
        total_files (int): Total number of files.
        num_unsupported_format_files (int): Number of files with unsupported format.
        num_files_with_errors (int): Number of files with errors.
        skipped_chunks (int): Number of chunks skipped.
    """
    chunks: List[Document]
    total_files: int
    num_unsupported_format_files: int = 0
    num_files_with_errors: int = 0
    num_files_skipped: int = 0
    skipped_chunks: int = 0


@dataclass_json
@dataclass
class DocumentIngestion:
    """
    Track the document versions being ingested.

    CREATE TABLE public.document_ingestion (
        id serial NOT NULL,
        url varchar NOT NULL,
        checksum varchar NOT NULL,
        size int4 NULL,
        staging_path varchar NULL,
        extraction_service_checksum varchar null,
        structured_content varchar null,
        embedding_service_checksum varchar null,
        embedding varchar null,
        status varchar null,
        error varchar null,
        created_dt varchar NOT null,
        updated_dt varchar null,
        CONSTRAINT document_ingestion_pk PRIMARY KEY (id)
    );

    CREATE UNIQUE INDEX document_ingestion_idx on document_ingestion (url, checksum);

    ALTER TABLE document_ingestion 
    ADD CONSTRAINT unique_document_ingestion_id
    UNIQUE USING INDEX document_ingestion_idx;
    """
    id: int = field(init=False, metadata=config(exclude=lambda x:True))
    url: str
    staging_path: str
    # id: Optional[int] = None
    checksum: Optional[str] = None
    size: Optional[int] = 0
    extraction_service_checksum: Optional[str] = None
    structured_content: Optional[str] = None
    embedding_service_checksum: Optional[str] = None
    embedding: Optional[str] = None
    # index_service_checksum: Optional[str] = None
    # document: Optional[Any] = None
    status: Optional[str] = None
    error: Optional[str] = None
    created_dt: Optional[str] = None
    updated_dt: Optional[str] = None

    # @property
    # def fingerprint(self):
    #     return hashlib.md5(str(self.to_dict()).encode()).hexdigest()

    def calculate_checksum(self):
        hash_md5 = hashlib.md5()
        with open(self.staging_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        self.checksum = hash_md5.hexdigest()

    def calculate_size(self):
        file_stats = os.stat(self.staging_path)
        self.size = file_stats.st_size

    def __post_init__(self):
        self.calculate_checksum()
        self.calculate_size()


document_ingestion = Table(
    "document_ingestion",
    metadata_obj,
    Column("id", Integer, primary_key=True),
    Column("url", String()),
    Column("staging_path", String()),
    Column("checksum", String()),
    Column("size", Integer()),
    Column("extraction_service_checksum", String(32)),
    Column("structured_content", String()),
    Column("embedding_service_checksum", String(32)),
    Column("embedding", String()),
    # Column("index_service_checksum", String(32)),
    # Column("document", String()),
    Column("status", String(10)),
    Column("error", String()),
    Column("created_dt", String(50)),
    Column("updated_dt", String(50))
)

mapper_registry.map_imperatively(DocumentIngestion, document_ingestion)
