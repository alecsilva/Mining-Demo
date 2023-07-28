# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import pymongo

from scrapy import settings
from scrapy.exceptions import DropItem
import logging



class MongoDBPipeline(object):
    collection_name = "questions"

    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
    
    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get("MONGODB_SERVER"),
            mongo_db=crawler.settings.get("MONGODB_DB", "items"),
        )
    
    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]   

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        self.db[self.collection_name].insert_one(ItemAdapter(item).asdict())
        return item 

    # def process_item(self, item, spider):
    #     valid = True
    #     for data in item:
    #         if not data:
    #             valid = False
    #             raise DropItem("Missing {0}!".format(data))
    #         if valid:
    #             self.collection.insert(dict(item))
    #             logging.msg("Question added to MongoDB database!",
    #                     level=logging.DEBUG, spider=spider)
    #         return item

#class StackPipeline:
#    def process_item(self, item, spider):
#        return item
