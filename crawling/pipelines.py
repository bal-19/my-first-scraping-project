# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import scrapy
import os
import s3fs
from scrapy.http import Request
from crawling.items import LpdpItem
from scrapy.pipelines.files import FilesPipeline


class CrawlingPipeline:
    def process_item(self, item, spider):
        return item

class MyFilesPipeline(FilesPipeline):
    def file_path(self, request, response=None, info=None, *, item=None):
        return request.url.split('/')[-1]

    def get_media_requests(self, item, info):
        file_url = item['file_url']
        yield Request(file_url)
        
    def item_completed(self, results, item, info):
        file_info = [result for result in results if result[0]]
        if file_info:
            file_path, file_url = file_info[0]
            source = item['source']
            sub_source = item['sub_source']
            file_name = item['file_url'].split('/')[-1]
            local_path = f'{item['local_path']}\{file_name}'
            self.upload_to_s3(local_path, file_name, source, sub_source)
        return item

    def upload_to_s3(self, local_path, file_name, source, sub_source):
        client_kwargs = {
            'key': '',
            'secret': '',
            'endpoint_url': '',
            'anon': False
        }

        s3_path = f'ai-pipeline-statistics/data/data_raw/data_laporan-lembaga-negara/{source}/{sub_source}/pdf/{file_name}'
        
        # Buat instance S3FileSystem
        s3 = s3fs.core.S3FileSystem(**client_kwargs)

        # Upload file ke S3
        s3.upload(rpath=s3_path, lpath=local_path)
    