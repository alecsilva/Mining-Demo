from scrapy import Spider
from scrapy.selector import Selector

from stack.items import StackItem

class StackSpider(Spider):
    name = "stack"
    allowed_domains = ["stackoverflow.com"]
    start_urls = [
        "http://stackoverflow.com/questions?pagesize=50&sort=newest",
    ]

    def parse(self, response):
        questions = Selector(response).xpath('//div[@class="s-post-summary--content"]/h3')

        for question in questions:
            items = StackItem()
            items['title'] = question.xpath('a[@class="s-link"]/text()').extract()
            items['url'] = question.xpath('a[@class="s-link"]/@href').extract()
            yield items

            