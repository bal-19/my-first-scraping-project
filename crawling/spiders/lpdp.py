import os
import s3fs
import json
import scrapy
from datetime import datetime
from crawling.items import LpdpItem


class LpdpSpider(scrapy.Spider):
    name = "lpdp"
    source = "Lembaga Pengelola Dana Pendidikan"
    data_type = "laporan lembaga negara"
    
    allowed_domains = ["lpdp.kemenkeu.go.id"]
    start_urls = [
        "https://lpdp.kemenkeu.go.id/en/informasi/laporan-tahunan/",
        "https://lpdp.kemenkeu.go.id/en/informasi/laporan-keuangan/",
        "https://lpdp.kemenkeu.go.id/en/informasi/laporan-kinerja/",
        "https://lpdp.kemenkeu.go.id/en/informasi/laporan-lain/"
    ]
    custom_settings = {
        'USER_AGENT' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
    }
    
    def saving_to_json(self, url_src, domain, tag, ctime, cepoch, praw, type, source, data_name, pdata_pdf, title, local_path):
        data = {
            "link": url_src,
            "domain": domain,
            "tag": tag,
            "crawling_time": ctime,
            "crawling_time_epoch": cepoch,
            "path_data_raw": praw,
            "type": type,
            "source": source,
            "data_name": data_name,
            "path_data_pdf": pdata_pdf,
            "title": title
        }
        
        # save to json
        with open(local_path, 'w') as f:
            json.dump(data, f)
            
    def upload_to_s3(self, local_path, raw_path):
        client_kwargs = {
            'key': '',
            'secret': '',
            'endpoint_url': '',
            'anon': False
        }
        
        # Buat instance S3FileSystem
        s3 = s3fs.core.S3FileSystem(**client_kwargs)

        # Upload file ke S3
        s3.upload(rpath=raw_path, lpath=local_path)
    
    def request(self):
        for url in self.start_urls:
            yield scrapy.Request(url=url, callback=self.parse)
            
    def parse(self, response):
        link_src = response.url
        sub_link_src = link_src.split('/')[5]
        domain = link_src.split('/')[2]
        sub_domain = domain.split('.')[0]
        data_name = response.css('h1.section-title-huge::text').get()
        container = response.css('div.ant-col.ant-col-24.ant-col-md-6')
        if container:
            tag = [sub_domain, domain, sub_link_src]
            for card in container:
                crawling_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                crawling_epoch = int(datetime.strptime(crawling_time, '%Y-%m-%d %H:%M:%S').timestamp())
                title = response.css('div > a > h3::text').get()
                download_link = card.css('div > a::attr(href)').get()
                
                s3_raw_path = f's3://ai-pipeline-statistics/data/data_raw/data_laporan-lembaga-negara/{self.source.lower().replace(' ', '-')}/{download_link.split('/')[-2]}'
                file_name = download_link.split('/')[-1]
                json_name = f'{file_name.replace('.pdf', '')}.json'
                s3_json_path = f'{s3_raw_path}/json/{json_name}'
                s3_pdf_path = f'{s3_raw_path}/pdf/{file_name}'
                local_path = f'F:\Work\Garapan gweh\crawling\{self.name}'
                
                if not os.path.exists(f'{local_path}/json'):
                    os.makedirs(f'{local_path}/json')
                self.saving_to_json(link_src, domain, tag, crawling_time, crawling_epoch, s3_json_path, self.data_type, self.source, data_name, s3_pdf_path, title, f'{local_path}/json/{json_name}')
                self.upload_to_s3(f'{local_path}/json/{json_name}', s3_json_path.replace('s3://', ''))
                
                yield scrapy.Request(url=download_link, callback=self.download_file)
                
    def download_file(self, response):
        pdf_dir = f'F:\Work\Garapan gweh\crawling\{self.name}\pdf'
        if not os.path.exists(pdf_dir):
            os.makedirs(pdf_dir)
        item = LpdpItem()
        item['local_path'] = pdf_dir
        item['file_url'] = response.url
        item['source'] = self.source.lower().replace(' ', '-')
        item['sub_source'] = response.url.split('/')[-2]
        yield item
        
