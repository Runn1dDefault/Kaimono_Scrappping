import json
import math
import re
import unicodedata
from typing import Iterable, Any

import psycopg2
import scrapy
from itemloaders import ItemLoader
from scrapy import Request
from scrapy.loader import ItemLoader
from scrapy.http import Response

from kaimono.items import (
    CategoryItem, TagItem,
    ProductItem, ProductCategoryItem,
    ProductInventoryTagItem, ProductInventoryItem, ProductImageItem
)
from kaimono.settings import DATABASE_SETTINGS
from kaimono.utils import (
    build_uniqlo_id, category_ids_for_scrape, get_site_id_from_db_id,
    delete_product_exclude_images
)


class UniqloCategorySpider(scrapy.Spider):
    name = 'uniqlo_category'
    custom_settings = {
        'LOG_LEVEL': 'INFO',
        'RETRY_ENABLED': True,
        'COOKIES_ENABLED': False
    }
    API_URL = 'https://www.uniqlo.com/us/api/commerce/v5/en/products/taxonomies?httpFailure=true'

    def start_requests(self) -> Iterable[Request]:
        yield Request(self.API_URL, callback=self.parse)

    def parse(self, response: Response, **kwargs: Any) -> Any:
        response_data = json.loads(response.body)
        result = response_data['result']
        yield from self.load_categories(result['genders'], 1)
        yield from self.load_categories(result['classes'], 2)
        yield from self.load_categories(result['categories'], 3)

    @staticmethod
    def load_categories(categories, level: int):
        loader = ItemLoader(CategoryItem())
        for category in categories:
            loader.add_value('id', build_uniqlo_id(category['id']))
            loader.add_value('name', category['name'])
            loader.add_value('level', level)

            parent_site_ids = [parent['id'] for parent in category.get('parents') or []]
            if parent_site_ids:
                loader.add_value('parent_id', build_uniqlo_id(parent_site_ids[-1]))
        yield loader.load_item()


def slugify(value, allow_unicode=False):
    value = str(value)
    if allow_unicode:
        value = unicodedata.normalize("NFKC", value)
    else:
        value = (
            unicodedata.normalize("NFKD", value)
            .encode("ascii", "ignore")
            .decode("ascii")
        )
    value = re.sub(r"[^\w\s-]", "", value.lower())
    return re.sub(r"[-\s]+", "-", value).strip("-_")


class UniqloSpider(scrapy.Spider):
    name = "uniqlo"
    custom_settings = {
        'LOG_LEVEL': 'INFO',
        'RETRY_ENABLED': True,
        'COOKIES_ENABLED': False
    }
    PAGE_LIMIT = 36
    BASE_API_URL = "https://www.uniqlo.com/us/api/commerce/v5/en/products"
    LIST_API_URL = BASE_API_URL + ("?categoryId={genre_id}"
                                   "&offset={offset}&limit={limit}&httpFailure=true")
    DETAIL_API_URL = BASE_API_URL + ("/{product_id}/price-groups/{price_group}/details"
                                     "?includeModelSize=true&httpFailure=true"
                                     "&withPrices=true&withStocks=true")
    PRICES_API_URL = BASE_API_URL + ("/{product_id}/price-groups/{price_group}/l2s"
                                     "?withPrices=true&withStocks=true&includePreviousPrice=false&httpFailure=true")

    ITEM_URL = "https://www.uniqlo.com/us/en/products/{item_code}/"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.psql_conn = psycopg2.connect(**DATABASE_SETTINGS)
        self.category_ids = category_ids_for_scrape(self.psql_conn, "uniqlo")

    def start_requests(self) -> Iterable[Request]:
        for category_id in self.category_ids:
            site_category_id = get_site_id_from_db_id(category_id)
            yield scrapy.Request(
                url=self.LIST_API_URL.format(
                    genre_id=site_category_id,
                    offset=0,
                    limit=self.PAGE_LIMIT
                ),
                callback=self.parse,
                meta={"category_id": category_id}
            )

    def parse(self, response: Response, **kwargs: Any) -> Any:
        yield from self.parse_items(response)
        yield from self.parse_pages(response)

    def parse_items(self, response: Response):
        response_data = json.loads(response.body)['result']
        for item in response_data['items']:
            item_code = item['productId']
            price_group = item['priceGroup']
            yield scrapy.Request(
                url=self.DETAIL_API_URL.format(product_id=item_code, price_group=price_group),
                callback=self.parse_product_items,
                meta={"item_code": item_code, "price_group": price_group} | response.meta
            )

    def parse_product_items(self, response: Response):
        response_data = json.loads(response.body)['result']
        item_code = response.meta['item_code']
        product_id = build_uniqlo_id(item_code)

        loader = ItemLoader(ProductItem())
        loader.add_value("id", product_id)
        loader.add_value("name", response_data['name'])
        loader.add_value("description", response_data['longDescription'])
        loader.add_value("site_avg_rating", response_data['rating'].get('average', 0.0))
        loader.add_value("site_reviews_count", response_data['rating'].get('count', 0))
        loader.add_value("shop_url", "https://www.uniqlo.com")
        loader.add_value("shop_code", "uniqlo")
        loader.add_value("can_choose_tags", True)
        yield loader.load_item()

        item_image_loader = ItemLoader(ProductImageItem())
        images_data = response_data['images']

        image_urls = [image_data["image"] for image_data in images_data['main'].values() if image_data.get("image")]
        if not image_urls:
            image_urls = [image_data["image"] for image_data in images_data['sub'] if image_data.get("image")]

        delete_product_exclude_images(self.psql_conn, product_id=product_id, image_urls=image_urls)

        for img_link in image_urls:
            item_image_loader.add_value("product_id", product_id)
            item_image_loader.add_value("url", img_link)

        yield item_image_loader.load_item()

        breadcrumbs = response_data['breadcrumbs']
        # sometimes categories in key "subcategory" come
        #   that were not saved into uniqlo_category
        subcategory_data = breadcrumbs.get('subcategory')
        if subcategory_data:
            subgenre_loader = ItemLoader(CategoryItem())
            subgenre_loader.add_value('id', build_uniqlo_id(subcategory_data['id']))
            subgenre_loader.add_value('name', subcategory_data['locale'])
            subgenre_loader.add_value('parent_id', build_uniqlo_id(breadcrumbs['category']['id']))
            subgenre_loader.add_value('level', subcategory_data['level'])
            yield subgenre_loader.load_item()

        product_category_loader = ItemLoader(ProductCategoryItem())
        for genre_data in breadcrumbs.values():
            product_category_loader.add_value("product_id", product_id)
            product_category_loader.add_value("category_id", build_uniqlo_id(genre_data['id']))
        yield product_category_loader.load_item()

        group_loader = ItemLoader(TagItem())
        tags_loader = ItemLoader(TagItem())

        for tag in response_data.get('tags') or []:
            group_id = build_uniqlo_id(tag['group'])
            group_loader.add_value("id", group_id)
            group_loader.add_value("name", tag["groupName"])

            tag_id = build_uniqlo_id(f"{tag['group']}:{tag['tag']}")
            tags_loader.add_value("id", tag_id)
            tags_loader.add_value("name", tag["tagName"])
            tags_loader.add_value("group_id", group_id)

        yield group_loader.load_item()
        yield tags_loader.load_item()

        response.meta['colors'] = response_data['colors']
        response.meta['item_images'] = response_data['images']
        response.meta['color_images'] = response_data['images']['chip']
        response.meta['sizes'] = response_data['sizes']
        response.meta['db_product_id'] = product_id
        yield scrapy.Request(
            url=self.PRICES_API_URL.format(
                product_id=item_code,
                price_group=response.meta['price_group']
            ),
            callback=self.parse_item_prices,
            meta=response.meta
        )

    def parse_item_prices(self, response: Response):
        product_id = response.meta['db_product_id']
        item_code = response.meta['item_code']

        color_images = response.meta['color_images']
        colors = {color['displayCode']: color['name'] for color in response.meta['colors']}
        sizes = {size['displayCode']: size['name'] for size in response.meta['sizes']}

        response_data = json.loads(response.body)['result']
        combines = response_data['l2s']
        stocks = response_data['stocks']
        prices = response_data['prices']

        group_loader = ItemLoader(TagItem())
        group_loader.add_value("id", "uniqlo_s1izes")
        group_loader.add_value("name", "Sizes")
        group_loader.add_value("id", "uniqlo_c1olors")
        group_loader.add_value("name", "Colors")
        yield group_loader.load_item()

        tag_loader = ItemLoader(TagItem())
        inventory_loader = ItemLoader(ProductInventoryItem())
        inventory_tag_loader = ItemLoader(ProductInventoryTagItem())
        processed_ids = set()

        for data in combines:
            combine_id = data['l2Id']
            color_code = data['color']['displayCode']
            stock_data = stocks[combine_id]
            price_data = prices[combine_id]

            color_name, size_name = colors[color_code], sizes[data['size']['displayCode']]
            color_slug, size_slug = slugify(color_name), slugify(size_name)
            color_tag_id, size_tag_id = build_uniqlo_id(color_slug), build_uniqlo_id(size_slug)

            if color_tag_id not in processed_ids:
                tag_loader.add_value("id", color_tag_id)
                tag_loader.add_value("name", color_name)
                tag_loader.add_value("group_id", "uniqlo_c1olors")
                processed_ids.add(color_tag_id)

            if size_tag_id not in processed_ids:
                tag_loader.add_value("id", size_tag_id)
                tag_loader.add_value("name", size_name)
                tag_loader.add_value("group_id", "uniqlo_s1izes")
                processed_ids.add(size_tag_id)

            inventory_id = f"{product_id}-{color_slug}-{size_slug}"
            inventory_loader.add_value("id", inventory_id)
            inventory_loader.add_value("product_id", product_id)
            inventory_loader.add_value("item_code", product_id)
            inventory_loader.add_value("site_price", price_data['base']['value'])
            inventory_loader.add_value("product_url", self.ITEM_URL.format(item_code=item_code))
            inventory_loader.add_value("name", f"Color: {color_name} Size: {size_name}")
            inventory_loader.add_value("quantity", stock_data['quantity'])
            inventory_loader.add_value("status_code", stock_data['statusLocalized'])
            color_image = color_images.get(color_code)
            if color_image:
                inventory_loader.add_value("color_image", color_image)
            else:
                inventory_loader.add_value("color_image", "")

            if (color_tag_id, size_tag_id, inventory_id) not in processed_ids:
                inventory_tag_loader.add_value("productinventory_id", inventory_id)
                inventory_tag_loader.add_value("tag_id", size_tag_id)
                inventory_tag_loader.add_value("productinventory_id", inventory_id)
                inventory_tag_loader.add_value("tag_id", color_tag_id)
                processed_ids.add((color_tag_id, size_tag_id, inventory_id))

        yield inventory_loader.load_item()
        yield tag_loader.load_item()
        yield inventory_tag_loader.load_item()

    def parse_pages(self, response: Response):
        pagination_data = json.loads(response.body)['result']['pagination']
        total = pagination_data['total']

        if total > self.PAGE_LIMIT:
            pages_count = math.ceil(total / self.PAGE_LIMIT)
            category_id = get_site_id_from_db_id(response.meta['category_id'])
            for page in range(2, pages_count + 1):
                url = self.LIST_API_URL.format(
                    genre_id=category_id,
                    offset=pagination_data['offset'] + self.PAGE_LIMIT,
                    limit=self.PAGE_LIMIT
                )
                yield scrapy.Request(
                    url=url,
                    callback=self.parse_items,
                    meta=response.meta
                )
        else:
            self.logger.info(
                "Genre: %s, Total: %s, Page LIMIT: %s, Offset: %s" % (
                    response.meta['category_id'],
                    total,
                    self.PAGE_LIMIT,
                    pagination_data['offset']
                )
            )
