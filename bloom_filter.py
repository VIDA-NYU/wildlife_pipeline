#!/usr/bin/env python3
# coding: utf-8

from pybloom import ScalableBloomFilter


class BloomFilter:
    def __init__(self, minio_client, file_name):
        self.minio = minio_client
        self.bloom_name = file_name
        if minio_client.check_obj_exists("bloom-filter", file_name):
            self.bloom = minio_client.get_bloom_filter(file_name)
        else:
            self.bloom = ScalableBloomFilter(mode=ScalableBloomFilter.SMALL_SET_GROWTH, error_rate=0.001)

    def save(self):
        self.minio.save_bloom(self.bloom, self.bloom_name)
    
    def load_bloom_filter(self):
        self.bloom = self.minio.get_bloom_filter(self.bloom_name)

    def check_bloom_filter(self, text: str) -> bool:
        if self.bloom:
            try:
                text = text.strip().lower()
                res = text in self.bloom
                if not res:
                    self.bloom.add(text)
                return res
            except UnicodeEncodeError:
                return True
