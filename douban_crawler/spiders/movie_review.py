# -*- coding: utf-8 -*-
import scrapy
import json

from douban_crawler.items import MovieReviewItem


class MovieReviewSpider(scrapy.Spider):
    name = 'movie_review'
    allowed_domains = ['api.douban.com']

    def start_requests(self):
        # 豆瓣 apikey
        apikey = '0b2bdeda43b5688921839c8ecb20399b'
        # 每页查询数量
        count = 10

        for i in range(50):
            start = i * count
            url = 'https://api.douban.com/v2/movie/subject/26266893/reviews?apikey={0}&start={1}&count={2}'.format(
                apikey, start, count)
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        body = response.text
        items = json.loads(body)

        for review in items['reviews']:
            item = MovieReviewItem()
            item['id'] = review['id']
            item['alt'] = review['alt']
            item['subject_id'] = review['subject_id']
            item['title'] = review['title']
            item['summary'] = review['summary']
            item['share_url'] = review['share_url']
            item['content'] = review['content']
            item['author_uid'] = review['author']['uid']
            item['rating'] = review['rating']['value']
            item['useful_count'] = review['useful_count']
            item['useless_count'] = review['useless_count']
            item['comments_count'] = review['comments_count']
            item['created_at'] = review['created_at']
            item['updated_at'] = review['updated_at']

            # print(item)
            # print(review)

            yield item
