import json
import logging

import requests


def collection_starting_from_page(page):
    url = f'https://stepik.org:443/api/courses?page={page}'
    response = requests.get(url)
    result = response.json()
    if response.status_code == 200:
        while result['meta']['has_next']:
            url = f'https://stepik.org:443/api/courses?page={page}'
            response = requests.get(url)
            result = response.json()
            courses = result['courses']

            with open(f'data.json', 'a') as json_file:
                for cours in courses:
                    data = {
                        'id': cours['id'],
                        'learners_count': cours['learners_count'],
                        'is_popular': cours['is_popular'],
                        'page': result['meta']['page']
                    }
                    json.dump(data, json_file)
                    json_file.write('\n')

            logging.info(f'Collected page {page}')
            page += 1
    else:
        logging.error("Something went wrong")


collection_starting_from_page(1)
