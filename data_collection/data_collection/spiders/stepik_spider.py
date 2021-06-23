import json

import scrapy
from decouple import config

BASE_DIR = config('BASE_DIR')

class StepikSpider(scrapy.Spider):
    name = "stepik"
    custom_settings = {
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

        with open(BASE_DIR + '/data_processing/data.json', 'a') as json_file:
            for cours in courses:
                data = {
                    'id': cours['id'],
                    'learners_count': cours['learners_count'],
                    'is_popular': cours['is_popular'],
                }
                json.dump(data, json_file)
                json_file.write('\n')

        while results['meta']['has_next']:
            next_url = f'https://stepik.org:443/api/courses?page={next_page}'
            yield response.follow(next_url, callback=self.parse)




