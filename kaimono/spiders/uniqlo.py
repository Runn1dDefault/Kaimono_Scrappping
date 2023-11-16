import json
from typing import Iterable, Any

import scrapy
from itemloaders import ItemLoader
from scrapy import Request
from scrapy.loader import ItemLoader
from scrapy.http import Response

from kaimono.items import CategoryItem, ProductItem


class UniqloCategoriesSpider(scrapy.Spider):
    name = 'uniqlo_category'
    API_URL = 'https://www.uniqlo.com/us/api/commerce/v5/en/products/taxonomies?httpFailure=true'
    
    def start_requests(self) -> Iterable[Request]:
        yield Request(self.API_URL, callback=self.parse)
        
    def parse(self, response: Response, **kwargs: Any) -> Any:
        response_data = json.loads(response.body)
        result = response_data['result']
        yield from self.load_categories(result['categories'])
        
    @staticmethod
    def load_categories(categories):
        for category in categories:
            loader = ItemLoader(CategoryItem())
            loader.add_value('site_id', category['id'])
            loader.add_value('name', category['name'])
            
            for parent in category.get('parents') or []:
                loader.add_value(
                    'parents',
                    {'site_id': parent['id'], 'name': parent['name']}
                )
                
            yield loader.load_item()
            
        
class UniqloSpider(scrapy.Spider):
    name = "uniqlo"
    start_urls = ["https://example.com"]

    def parse(self, response, **kwargs):
        pass
    
    