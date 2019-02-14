# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class MovieCommentItem(scrapy.Item):
    id = scrapy.Field()
    subject_id = scrapy.Field()
    content = scrapy.Field()
    author_uid = scrapy.Field()
    rating = scrapy.Field()
    useful_count = scrapy.Field()
    created_at = scrapy.Field()
    updated_at = scrapy.Field()


class MovieReviewItem(scrapy.Item):
    id = scrapy.Field()
    alt = scrapy.Field()
    subject_id = scrapy.Field()
    title = scrapy.Field()
    summary = scrapy.Field()
    share_url = scrapy.Field()
    content = scrapy.Field()
    author_uid = scrapy.Field()
    rating = scrapy.Field()
    useful_count = scrapy.Field()
    useless_count = scrapy.Field()
    comments_count = scrapy.Field()
    created_at = scrapy.Field()
    updated_at = scrapy.Field()
