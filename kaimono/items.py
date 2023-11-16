from decimal import Decimal

import scrapy
from scrapy.loader.processors import TakeFirst


class CategoryItem(scrapy.Item):
    site_id = scrapy.Field(
        serializer=int,
        output_processor=TakeFirst()
    )
    name = scrapy.Field(
        serializer=str,
        output_processor=TakeFirst()
    )
    parents = scrapy.Field(serializer=int)


class ProductItem(scrapy.Item):
    site_id = scrapy.Field(output_processor=TakeFirst())
    name = scrapy.Field(serializer=str, output_processor=TakeFirst())
    description = scrapy.Field(serializer=str, output_processor=TakeFirst())
    site_price = scrapy.Field(serializer=Decimal, output_processor=TakeFirst())
