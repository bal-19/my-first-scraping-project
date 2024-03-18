import os
import s3fs
import json
import scrapy
from datetime import datetime


class EaspirasiJatengSpider(scrapy.Spider):
    name = "easpirasi_jateng"
    start_urls = ["https://easpirasi.dprd.jatengprov.go.id"]
    
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
            
    def saving_to_json(self, link, domain, tag, ctime, cepoch, praw, tipe, src, data_name, detail_data, local_path):
        data = {
            "link": link,
            "domain": domain,
            "tag": tag,
            "crawling_time": ctime,
            "crawling_time_epoch": cepoch,
            "path_data_raw": praw,
            "type": tipe,
            "source": src,
            "data_name": data_name,
            "detail_aspirasi": detail_data
        }
        
        # save to json
        with open(local_path, 'w') as f:
            json.dump(data, f)

    def parse(self, response):
        link = response.url
        domain = link.split('/')[2]
        subdomain = link.split('/')[2].split('.')[0]
        tipe = 'Data ICC'
        source = 'e-aspirasi dprd jateng'
        data_name = 'data daftar aspirasi'
        tag = [domain, subdomain, source]
        
        table = response.css('#list > div > table')
        
        for trb in table.css('tbody > tr'):
            ctime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            cepoch = int(datetime.strptime(ctime, '%Y-%m-%d %H:%M:%S').timestamp())
            td = trb.css('td').extract()
            td = [cell.replace('<td>', '').replace('<td style="text-align:left">', '').replace('</td>', '').strip() for cell in td]
            data = []
            data.append({
                'nama' : td[1],
                'komisi' : td[2],
                'isu_strategi' : td[3],
                'urusan_daerah' : td[4],
                'skpd' : td[5],
                'aspirasi' : td[6],
                'kepada' : td[7],
                'anggaran' : td[8],
                'sasaran' : td[9],
                'lokasi' : td[10],
            })
            
            json_name = f'{data_name.replace(' ', '_')}_{td[0]}_{cepoch}.json'
            path_s3 = f's3://ai-pipeline-statistics/data/data_raw/data_icc/{source.replace(' ', '_')}/json/{json_name}'
            local_path = f'f:/Work/Garapan gweh/html/crawling/aspirasi jateng/{json_name}'
            
            self.saving_to_json(link, domain, tag, ctime, cepoch, path_s3, tipe, source, data_name, data, local_path)
            self.upload_to_s3(local_path, path_s3.replace('s3://', ''))