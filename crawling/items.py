# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class CrawlingItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass

class LpdpItem(scrapy.Item):
    file_url = scrapy.Field()
    source = scrapy.Field()
    local_path = scrapy.Field()
    sub_source = scrapy.Field()