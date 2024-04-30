#!/usr/bin/env python
# coding: utf-8

from typing import Dict, Any, List

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan

class ElasticSearch():
    def __init__(self, host: str, index_start: str):
        self.es_host = host
        self.index_start = index_start
        self.client = Elasticsearch(host)

    def get_client(self):
        return self.client

    def get_index_doc_count(self, index: str):
        es_client = self.get_client()
        return es_client.count(index=index).get('count')

    def get_docs(self, end_date: str, start_date: str):
        print("Extracting Data from ES")
        docs = scan(
                client=self.get_client(),
                index=self.index_start,
                scroll="60m",
                query= { "query":{
                            "bool": {
                                "filter": {
                                    "range": {
                                        "retrieved": {
                                            "gte": start_date,
                                            "lt": end_date,
                                        },
                                    },
                                },

                                "must_not": [
                                    {
                                            "match_phrase": {

                                                    "text": "Result not found"
                                            }
                                    },
                                    {
                                            "match_phrase": {
                                                    "text":  "No exact matches found"
                                              }
                                    },
                                     {
                                            "match_phrase": {
                                                    "text":  "Not found"
                                              }
                                    },
                                    {
                                            "match_phrase": {
                                                    "text":  "Item Not found"
                                              }
                                    },
                                ],
                            }
                        }
                        })
        import json

        output_file = "output.jsonl"  

        with open(output_file, "w") as f:
            for doc in docs:
                f.write(json.dumps(doc) + "\n")

        return output_file

    def send_to_new_index(self, docs: List[Dict]) -> Dict[str, Any]:
        raise NotImplementedError
        # def gendata(docs: List[Dict]):
        #     for doc in docs:
        #         yield {
        #             '_index': self.index_end,
        #             '_id': doc["url"],
        #             '_source': doc
        #         }

        # print("Starting dump")
        # resp = helpers.bulk(
        #     self.client,
        #     gendata(docs),
        #     index = self.index_end,
        #     request_timeout=30
        # )

