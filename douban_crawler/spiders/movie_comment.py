# -*- coding: utf-8 -*-
import scrapy
import json
import datetime

from douban_crawler.items import MovieCommentItem


class MovieCommentSpider(scrapy.Spider):
    name = 'movie_comment'
    allowed_domains = ['api.douban.com']

    def start_requests(self):
        # 豆瓣 apikey
        apikey = '0b2bdeda43b5688921839c8ecb20399b'
        # 每页查询数量
        count = 10

        for i in range(50):
            start = i * count
            url = 'https://api.douban.com/v2/movie/subject/26266893/comments?apikey={0}&start={1}&count={2}'.format(
                apikey, start, count)
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        body = response.text
        items = json.loads(body)

        for comment in items['comments']:
            item = MovieCommentItem()
            item['id'] = comment['id']
            item['subject_id'] = comment['subject_id']
            item['content'] = comment['content']
            item['author_uid'] = comment['author']['uid']
            item['rating'] = comment['rating']['value']
            item['useful_count'] = comment['useful_count']
            item['created_at'] = comment['created_at']
            item['updated_at'] = datetime.datetime.now()
            # print(item)
            # print(comment)

            yield item
