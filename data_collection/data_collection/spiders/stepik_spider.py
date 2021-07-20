import json

import scrapy
from decouple import config
from ..items import StepikItem
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))


class StepikSpider(scrapy.Spider):
    name = "stepik"
    custom_settings = {
        'FEEDS': {
            BASE_DIR + '/data_processing/data.json': {
                    'format': 'json'
                }
        },
        'JOBDIR': 'crawls/stepik-1',
        'CLOSESPIDER_PAGECOUNT': '10'
    }
    start_urls = [
        'https://stepik.org:443/api/courses?page=1',
    ]

    def parse(self, response):
        results = json.loads(response.body)
        next_page = results['meta']['page']+1
        courses = results['courses']

        for cours in courses:
            item = {
                'id': cours['id'],
                'learners_count': cours['learners_count'],
                'is_popular': cours['is_popular'],
            }
            yield item

        if results['meta']['has_next']:
            next_url = f'https://stepik.org:443/api/courses?page={next_page}'
            yield response.follow(next_url, callback=self.parse)




