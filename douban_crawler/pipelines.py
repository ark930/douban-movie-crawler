# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import jieba
import jieba.posseg as pseg
import scrapy_mysql_pipeline
from pymysql.cursors import DictCursor
from twisted.enterprise import adbapi
from collections import Counter
from douban_crawler.items import MovieCommentItem


class MysqlPipeline(scrapy_mysql_pipeline.MySQLPipeline):
    def __init__(self, crawler):
        self.stats = crawler.stats
        self.settings = crawler.settings
        db_args = {
            'host': self.settings.get('MYSQL_HOST', 'localhost'),
            'port': self.settings.get('MYSQL_PORT', 3306),
            'user': self.settings.get('MYSQL_USER', None),
            'password': self.settings.get('MYSQL_PASSWORD', ''),
            'db': self.settings.get('MYSQL_DB', None),
            'charset': 'utf8mb4',
            'cursorclass': DictCursor,
            'cp_reconnect': True,
        }
        self.retries = self.settings.get('MYSQL_RETRIES', 3)
        self.close_on_error = self.settings.get('MYSQL_CLOSE_ON_ERROR', True)
        self.upsert = self.settings.get('MYSQL_UPSERT', False)
        self.table = self.settings.get('MYSQL_TABLE', None)
        self.db = adbapi.ConnectionPool('pymysql', **db_args)

    def process_item(self, item, spider):
        if isinstance(item, MovieCommentItem):
            self.table = 'movie_comments'
        else:
            self.table = 'movie_reviews'

        super().process_item(item, spider)


class WordSegmentPipeline(object):
    def __init__(self):
        self.counter = Counter()
        self.counters = [Counter(), Counter(), Counter(), Counter(), Counter()]

        self.flags = {}

        self.all_file = None
        self.rating_files = [None, None, None, None, None]

        # 正面评价
        self.positive_comment_file = None
        # 负面评价
        self.negative_comment_file = None
        # 正面情感
        self.positive_sentiment_file = None
        # 负面情感
        self.negative_sentiment_file = None

        self.positive_comment_words = []
        self.negative_comment_words = []
        self.positive_sentiment_words = []
        self.negative_sentiment_words = []

    def open_spider(self, spider):
        # jieba.enable_parallel(4)
        jieba.load_userdict('userdict.txt')

        # 所有评论分词
        self.all_file = open('output/all.csv', 'w')
        header = 'Text;Size;Color;Angle;Font;Repeat;URL\n'
        self.all_file.write(header)

        # 根据评分分类存储
        for i in range(len(self.rating_files)):
            self.rating_files[i] = open('output/rating_{}.csv'.format(i + 1), 'w')
            self.rating_files[i].write(header)

        self.positive_comment_file = open('output/positive_comment.csv', 'w')
        self.positive_comment_file.write(header)

        self.negative_comment_file = open('output/negative_comment.csv', 'w')
        self.negative_comment_file.write(header)

        self.positive_sentiment_file = open('output/positive_sentiment.csv', 'w')
        self.positive_sentiment_file.write(header)

        self.negative_sentiment_file = open('output/negative_sentiment.csv', 'w')
        self.negative_sentiment_file.write(header)

        with open('data/sentiment/正面评价词语（中文）.txt', 'r') as f:
            for line in f.readlines():
                self.positive_comment_words.append(line.strip())

        with open('data/sentiment/负面评价词语（中文）.txt', 'r') as f:
            for line in f.readlines():
                self.negative_comment_words.append(line.strip())

        with open('data/sentiment/正面情感词语（中文）.txt', 'r') as f:
            for line in f.readlines():
                self.positive_sentiment_words.append(line.strip())

        with open('data/sentiment/负面情感词语（中文）.txt', 'r') as f:
            for line in f.readlines():
                self.negative_sentiment_words.append(line.strip())

    def close_spider(self, spider):
        self._write_file(self.all_file, self.counter)

        for i in range(len(self.counters)):
            self._write_file(self.rating_files[i], self.counters[i])

        positive_comment_counter = Counter()
        negative_comment_counter = Counter()
        positive_sentiment_counter = Counter()
        negative_sentiment_counter = Counter()
        for (k, v) in self.counter.most_common():
            if k in self.positive_comment_words:
                positive_comment_counter[k] = v
            if k in self.negative_comment_words:
                negative_comment_counter[k] = v
            if k in self.positive_sentiment_words:
                positive_sentiment_counter[k] = v
            if k in self.negative_sentiment_words:
                negative_sentiment_counter[k] = v

        self._write_file(self.positive_comment_file, positive_comment_counter)
        self._write_file(self.negative_comment_file, negative_comment_counter)
        self._write_file(self.positive_sentiment_file, positive_sentiment_counter)
        self._write_file(self.negative_sentiment_file, negative_sentiment_counter)

    def process_item(self, item, spider):
        content = item['content']
        rating = int(item['rating'])

        seg_list = pseg.cut(content, HMM=False)

        for word, flag in seg_list:
            if flag not in ['b', 'c', 'd', 'df', 'f', 'm', 'p', 'r', 'u'] \
                    and len(word) > 1 and word != '\r\n':
                self.counter[word] += 1
                self.counters[rating - 1][word] += 1
                self.flags[word] = flag

        return item

    def _write_file(self, file, counter):
        for (k, v) in counter.most_common():
            # print('%s%s  %d' % ('  ' * (5 - len(k)), k, v))
            line = '{};{};Random;Random;Random;Yes;{}\n'.format(k, v, self.flags[k])
            file.write(line)

        file.close()
