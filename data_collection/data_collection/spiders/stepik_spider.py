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
        page = results['meta']['page']
        with open(f'page_{page}.json', 'w') as json_file:
            json.dump(results, json_file)
        for result in results:
            try:
                next_url = f'https://stepik.org:443/api/courses?page={page+1}'
                yield response.follow(next_url, callback=self.parse)
            except:
                continue

