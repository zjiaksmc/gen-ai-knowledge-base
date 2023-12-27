"""Data Preparation Script for an Azure Cognitive Search Index."""
import argparse
import dataclasses
import json
import os
import subprocess

import requests
import time
from azure.core.credentials import AzureKeyCredential
from azure.search.documents import SearchClient
from tqdm import tqdm

from ..utils.document import chunk_directory, chunk_blob_container

SUPPORTED_LANGUAGE_CODES = {
    "ar": "Arabic",
    "hy": "Armenian",
    "eu": "Basque",
    "bg": "Bulgarian",
    "ca": "Catalan",
    "zh-Hans": "Chinese Simplified",
    "zh-Hant": "Chinese Traditional",
    "cs": "Czech",
    "da": "Danish",
    "nl": "Dutch",
    "en": "English",
    "fi": "Finnish",
    "fr": "French",
    "gl": "Galician",
    "de": "German",
    "el": "Greek",
    "hi": "Hindi",
    "hu": "Hungarian",
    "id": "Indonesian (Bahasa)",
    "ga": "Irish",
    "it": "Italian",
    "ja": "Japanese",
    "ko": "Korean",
    "lv": "Latvian",
    "no": "Norwegian",
    "fa": "Persian",
    "pl": "Polish",
    "pt-Br": "Portuguese (Brazil)",
    "pt-Pt": "Portuguese (Portugal)",
    "ro": "Romanian",
    "ru": "Russian",
    "es": "Spanish",
    "sv": "Swedish",
    "th": "Thai",
    "tr": "Turkish"
}


def create_or_update_search_index(
        service_endpoint,
        admin_key,
        index_name="default-index", 
        semantic_config_name="default", 
        credential=None, 
        language=None,
        vector_config_name=None
    ):
    
    if credential is None and admin_key is None:
        raise ValueError("credential and admin key cannot be None")
    
    url = f"{service_endpoint}/indexes/{index_name}?api-version=2023-11-01"
    headers = {
        "Content-Type": "application/json",
        "api-key": admin_key,
    }

    body = {
        "fields": [
            {
                "name": "id",
                "type": "Edm.String",
                "searchable": True,
                "key": True,
            },
            {
                "name": "content",
                "type": "Edm.String",
                "searchable": True,
                "sortable": False,
                "facetable": False,
                "filterable": False,
                "analyzer": f"{language}.lucene" if language else None,
            },
            {
                "name": "title",
                "type": "Edm.String",
                "searchable": True,
                "sortable": False,
                "facetable": False,
                "filterable": False,
                "analyzer": f"{language}.lucene" if language else None,
            },
            {
                "name": "filepath",
                "type": "Edm.String",
                "searchable": True,
                "sortable": False,
                "facetable": False,
                "filterable": False,
            },
            {
                "name": "url",
                "type": "Edm.String",
                "searchable": True,
            },
            {
                "name": "metadata",
                "type": "Edm.String",
                "searchable": True,
            },
        ],
        "suggesters": [],
        "scoringProfiles": [],
        "semantic": {
            "configurations": [
                {
                    "name": semantic_config_name,
                    "prioritizedFields": {
                        "titleField": {"fieldName": "title"},
                        "prioritizedContentFields": [{"fieldName": "content"}],
                        "prioritizedKeywordsFields": [],
                    },
                }
            ]
        },
    }

    if vector_config_name:
        body["fields"].append({
            "name": "contentVector",
            "type": "Collection(Edm.Single)",
            "searchable": True,
            "retrievable": True,
            "dimensions": 1536,
            "vectorSearchProfile": vector_config_name
        })

        body["vectorSearch"] = {
            "algorithms": [
                {
                    "name": "hnsm-fast",
                    "kind": "hnsw",
                    "hnswParameters": {
                        "m": 4,
                        "efConstruction": 400,
                        "efSearch": 500,
                        "metric": "cosine"
                    }
                },
                {
                    "name": "hnsm-complete",
                    "kind": "hnsw",
                    "hnswParameters": {
                        "m": 8,
                        "efConstruction": 800,
                        "efSearch": 800,
                        "metric": "cosine"
                    }
                }
            ],
            "profiles": [
                {
                    "name": vector_config_name,
                    "algorithm": "hnsm-fast",
                }
            ]
        }

    response = requests.put(url, json=body, headers=headers)
    if response.status_code == 201:
        print(f"Created search index {index_name}")
    elif response.status_code == 204:
        print(f"Updated existing search index {index_name}")
    else:
        print(response.text)
        raise Exception(f"Failed to create search index. Error: {response}")
        
    
    return True


def upload_documents_to_index(endpoint, index_name, docs, admin_key, credential=None, upload_batch_size = 50):
    if credential is None and admin_key is None:
        raise ValueError("credential and admin_key cannot be None")
    
    to_upload_dicts = []

    id = 0
    for d in docs:
        if type(d) is not dict:
            d = dataclasses.asdict(d)
        # add id to documents
        d.update({"@search.action": "upload", "id": str(id)})
        if "contentVector" in d and d["contentVector"] is None:
            del d["contentVector"]
        to_upload_dicts.append(d)
        id += 1

    search_client = SearchClient(
        endpoint=endpoint,
        index_name=index_name,
        credential=AzureKeyCredential(admin_key),
    )
    # Upload the documents in batches of upload_batch_size
    for i in tqdm(range(0, len(to_upload_dicts), upload_batch_size), desc="Indexing Chunks..."):
        batch = to_upload_dicts[i: i + upload_batch_size]
        results = search_client.upload_documents(documents=batch)
        num_failures = 0
        errors = set()
        for result in results:
            if not result.succeeded:
                print(f"Indexing Failed for {result.key} with ERROR: {result.error_message}")
                num_failures += 1
                errors.add(result.error_message)
        if num_failures > 0:
            raise Exception(f"INDEXING FAILED for {num_failures} documents. Please recreate the index."
                            f"To Debug: PLEASE CHECK chunk_size and upload_batch_size. \n Error Messages: {list(errors)}")

def validate_index(service_endpoint, index_name, admin_key):
    api_version = "2021-04-30-Preview"

    headers = {
        "Content-Type": "application/json", 
        "api-key": admin_key}
    params = {"api-version": api_version}
    url = f"{service_endpoint}/indexes/{index_name}/stats"
    for retry_count in range(5):
        response = requests.get(url, headers=headers, params=params)

        if response.status_code == 200:
            response = response.json()
            num_chunks = response['documentCount']
            if num_chunks==0 and retry_count < 4:
                print("Index is empty. Waiting 60 seconds to check again...")
                time.sleep(60)
            elif num_chunks==0 and retry_count == 4:
                print("Index is empty. Please investigate and re-index.")
            else:
                print(f"The index contains {num_chunks} chunks.")
                average_chunk_size = response['storageSize']/num_chunks
                print(f"The average chunk size of the index is {average_chunk_size} bytes.")
                break
        else:
            if response.status_code==404:
                print(f"The index does not seem to exist. Please make sure the index was created correctly, and that you are using the correct service and index names")
            elif response.status_code==403:
                print(f"Authentication Failure: Make sure you are using the correct key")
            else:
                print(f"Request failed. Please investigate. Status code: {response.status_code}")
            break

def create_index(config, credential, form_recognizer_client=None, embedding_model_endpoint=None, use_layout=False, njobs=4):
    search_service_endpoint = config.search_service_endpoint
    index_name = config.index_name
    language = config.language

    if language and language not in SUPPORTED_LANGUAGE_CODES:
        raise Exception(f"ERROR: Ingestion does not support {language} documents. "
                        f"Please use one of {SUPPORTED_LANGUAGE_CODES}."
                        f"Language is set as two letter code for e.g. 'en' for English."
                        f"If you donot want to set a language just remove this prompt config or set as None")

    # create or update search index with compatible schema
    admin_key = os.environ.get("AZURE_SEARCH_ADMIN_KEY", None)
    if not create_or_update_search_index(
        search_service_endpoint,
        admin_key,
        index_name,
        config.semantic_config_name,
        credential,
        language,
        vector_config_name=config.vector_config_name
        ):
        raise Exception(f"Failed to create or update index {index_name}")
    
    # data_configs = []
    # if "data_path" in config:
    #     data_configs.append({
    #         "path": config.data_path,
    #         "url_prefix": config.url_prefix,
    #     })
    # if "data_paths" in config:
    #     data_configs.extend(config.data_paths)

    # for data_config in data_configs:
    # chunk directory
    print(f"Chunking path {config.data_path}...")
    add_embeddings = False
    if config.vector_config_name and embedding_model_endpoint:
        add_embeddings = True

    if "blob.core" in config.data_path:
        result = chunk_blob_container(config.data_path, credential=credential, num_tokens=config.chunk_size, token_overlap=config.token_overlap,
                            azure_credential=credential, form_recognizer_client=form_recognizer_client, use_layout=use_layout, njobs=njobs,
                            add_embeddings=add_embeddings, embedding_endpoint=embedding_model_endpoint, url_prefix=config.url_prefix)
    elif os.path.exists(config.data_path):
        result = chunk_directory(config.data_path, num_tokens=config.chunk_size, token_overlap=config.token_overlap,
                                azure_credential=credential, form_recognizer_client=form_recognizer_client, use_layout=use_layout, njobs=njobs,
                                add_embeddings=add_embeddings, embedding_endpoint=embedding_model_endpoint, url_prefix=config.url_prefix)
    else:
        raise Exception(f"Path {config.data_path} does not exist and is not a blob URL. Please check the path and try again.")

    if len(result.chunks) == 0:
        raise Exception("No chunks found. Please check the data path and chunk size.")

    print(f"Processed {result.total_files} files")
    print(f"Unsupported formats: {result.num_unsupported_format_files} files")
    print(f"Files with errors: {result.num_files_with_errors} files")
    print(f"Found {len(result.chunks)} chunks")

    # upload documents to index
    print("Uploading documents to index...")
    upload_documents_to_index(search_service_endpoint, index_name, result.chunks, admin_key, credential)

    # check if index is ready/validate index
    print("Validating index...")
    validate_index(search_service_endpoint, index_name, admin_key)
    print("Index validation completed")


def valid_range(n):
    n = int(n)
    if n < 1 or n > 32:
        raise argparse.ArgumentTypeError("njobs must be an Integer between 1 and 32.")
    return n