import json
from pprint import pprint

import scrapy


class StepikSpider(scrapy.Spider):
    name = "stepik"
    start_urls = [
        'https://stepik.org:443/api/courses?page=1',
    ]

    def parse(self, response):
        results = json.loads(response.body)
        for result in results:
            try:
                page = results['meta']['page']
                next_url = f'https://stepik.org:443/api/courses?page={page}'
                yield response.follow(next_url, callback=self.parse_details)
            except:
                continue

    def parse_details(self, response):
        result = json.loads(response.body)
        return (result)

        with open('info.json', 'w') as json_file:
            json.dump(result, json_file)
