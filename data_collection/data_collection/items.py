# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class StepikItem(scrapy.Item):
    id = scrapy.Field()
    learners_count = scrapy.Field()
    is_popular = scrapy.Field()
