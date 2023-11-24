import json
import random
from typing import Any, Iterable

import psycopg2
import scrapy
from scrapy import Request
from scrapy.http import Response
from scrapy.loader import ItemLoader

from kaimono.items import CategoryItem, ProductItem, ProductCategoryItem, ProductImageItem, TagItem, ProductTagItem
from kaimono.settings import DATABASE_SETTINGS
from kaimono.utils import build_rakuten_id, category_ids_for_scrape, get_genres_tree, get_site_id_from_db_id, site_tags

RAKUTEN_BASE_URL = "https://app.rakuten.co.jp/"


def random_rakuten_app_id(app_ids):
    return random.choice(app_ids)


class RakutenCategorySpider(scrapy.Spider):
    name = "rakuten_category"
    RAKUTEN_APP_IDS = (
        "1006081949539677212",
        "1032684706123538391",
    )
    custom_settings = {
        'LOG_LEVEL': 'INFO',
        "DEFAULT_REQUEST_HEADERS": {
            "Content-Type": "application/json"
        },
        "CONCURRENT_REQUESTS": len(RAKUTEN_APP_IDS)
    }
    API_URL = RAKUTEN_BASE_URL + ("services/api/IchibaGenre/Search/20120723"
                                  "?applicationId={app_id}&formatVersion=2&genreId={genre_id}")
    start_urls = [API_URL.format(app_id=random_rakuten_app_id(RAKUTEN_APP_IDS), genre_id="0")]

    def parse(self, response: Response, **kwargs: Any) -> Any:
        response_data = json.loads(response.body)
        current_data = response_data['current']
        children_data = response_data.get('children')
        parents_data = response_data.get('parents')

        current_id = build_rakuten_id(current_data['genreId'])
        self.logger.info(f"Genre: {current_id}")
        current_loader = ItemLoader(CategoryItem())

        current_loader.add_value("id", current_id)
        current_loader.add_value("name", current_data['genreName'])
        if parents_data:
            current_loader.add_value("parent_id", build_rakuten_id(parents_data[-1]["genreId"]))
        current_loader.add_value("level", current_data['genreLevel'])
        yield current_loader.load_item()

        children_genres_loader = ItemLoader(CategoryItem())
        for child in children_data or []:
            child_id = child['genreId']
            children_genres_loader.add_value("id", build_rakuten_id(child_id))
            children_genres_loader.add_value("name", child['genreName'])
            children_genres_loader.add_value("parent_id", current_id)
            children_genres_loader.add_value("level", child['genreLevel'])
            yield scrapy.Request(
                url=self.API_URL.format(
                    app_id=random_rakuten_app_id(self.RAKUTEN_APP_IDS),
                    genre_id=child_id
                ),
                callback=self.parse
            )
        yield children_genres_loader.load_item()


class RakutenSpider(scrapy.Spider):
    name = "rakuten"
    RAKUTEN_APP_IDS = (
        "1006081949539677212",
        "1032684706123538391",
        "1027393930619954222",
    )

    custom_settings = {
        'LOG_LEVEL': 'INFO',
        "DEFAULT_REQUEST_HEADERS": {
            "Content-Type": "application/json"
        },
        "CONCURRENT_REQUESTS": len(RAKUTEN_APP_IDS) * 3,
    }

    API_URL = ("https://app.rakuten.co.jp/services/api/IchibaItem/Search/20220601"
               "?applicationId={app_id}&formatVersion=2&genreId={genre_id}&tagInformationFlag=1&page={page}")

    TAG_API_URL = RAKUTEN_BASE_URL + ("services/api/IchibaTag/Search/20140222?"
                                      "applicationId={app_id}&formatVersion=2&tagId={tag_id}")

    def __init__(self, *args, **kwargs):
        assert self.RAKUTEN_APP_IDS
        super().__init__(*args, **kwargs)
        self.psql_con = psycopg2.connect(**DATABASE_SETTINGS)
        self.category_ids = category_ids_for_scrape(self.psql_con, self.name)

    def start_requests(self) -> Iterable[Request]:
        for category_id in self.category_ids:
            site_category_id = get_site_id_from_db_id(category_id)
            yield scrapy.Request(
                url=self.API_URL.format(
                    app_id=random_rakuten_app_id(self.RAKUTEN_APP_IDS),
                    genre_id=site_category_id,
                    page=1
                ),
                callback=self.parse,
                meta={"category_id": category_id, "site_category_id": site_category_id, "page_num": 1}
            )

    def parse(self, response: Response, **kwargs: Any) -> Any:
        response_data = json.loads(response.body)

        tags_info = response_data['TagInformation']
        has_tags_info = bool(tags_info)
        if has_tags_info:
            yield from self.parse_tags(tags_info)

        category_id = response.meta['category_id']
        categories_tree = get_genres_tree(self.psql_con, category_id)

        product_loader = ItemLoader(ProductItem())
        category_loader = ItemLoader(ProductCategoryItem())
        image_loader = ItemLoader(ProductImageItem())
        product_tag_loader = ItemLoader(ProductTagItem())

        processed_product_ids = set()

        for item in response_data['Items']:
            item_id = build_rakuten_id(item["itemCode"])

            if item_id in processed_product_ids:
                continue

            product_loader.add_value("id", item_id)
            product_loader.add_value("name", item["itemName"])
            product_loader.add_value("description", item["itemCaption"])
            product_loader.add_value("site_price", item["itemPrice"])
            product_loader.add_value("site_avg_rating", item["reviewAverage"])
            product_loader.add_value("site_reviews_count", item["reviewCount"])
            product_loader.add_value("product_url", item["itemUrl"])

            for category_id in categories_tree:
                category_loader.add_value("product_id", item_id)
                category_loader.add_value("category_id", category_id)

            for url in item["mediumImageUrls"] or item["smallImageUrls"]:
                image_loader.add_value("product_id", item_id)
                image_loader.add_value("url", url)

            request_to_tags = []

            for tag_id in item["tagIds"]:
                db_tag_id = build_rakuten_id(tag_id)

                if db_tag_id not in site_tags(self.psql_con, self.name):
                    request_to_tags.append(
                        scrapy.Request(
                            url=self.TAG_API_URL.format(
                                app_id=random_rakuten_app_id(self.RAKUTEN_APP_IDS),
                                tag_id=tag_id
                            ),
                            callback=self.parse_tag,
                            meta={"product_id": item_id, 'tag_id': db_tag_id}
                        )
                    )
                    continue

                product_tag_loader.add_value("product_id", item_id)
                product_tag_loader.add_value("tag_id", db_tag_id)

            processed_product_ids.add(item_id)

        yield product_loader.load_item()
        yield category_loader.load_item()
        yield image_loader.load_item()
        yield product_tag_loader.load_item()

        for request in request_to_tags:
            yield request

        page_num = response.meta['page_num']
        pages_count = response_data['pageCount']

        if page_num < pages_count:
            response.meta['page_num'] += 1
            yield scrapy.Request(
                url=self.API_URL.format(
                    app_id=random_rakuten_app_id(self.RAKUTEN_APP_IDS),
                    genre_id=response.meta['site_category_id'],
                    page=response.meta['page_num']
                ),
                callback=self.parse,
                meta=response.meta
            )

    def parse_tag(self, response: Response):
        response_data = json.loads(response.body)
        yield from self.parse_tags(response_data['tagGroups'])

        product_tag_loader = ItemLoader(ProductTagItem())
        product_tag_loader.add_value("product_id", response.meta['product_id'])
        product_tag_loader.add_value("tag_id", response.meta['tag_id'])
        yield product_tag_loader.load_item()

    def parse_tags(self, tags_data):
        tag_group_loader = ItemLoader(TagItem())
        tag_loader = ItemLoader(TagItem())

        for tag_group_data in tags_data:
            group_id = build_rakuten_id(tag_group_data["tagGroupId"])
            tag_group_loader.add_value("id", group_id)
            tag_group_loader.add_value("name", tag_group_data["tagGroupName"])

            for tag_data in tag_group_data["tags"]:
                tag_loader.add_value("id", build_rakuten_id(tag_data["tagId"]))
                tag_loader.add_value("name", tag_data["tagName"])
                tag_loader.add_value("group_id", group_id)

        yield tag_group_loader.load_item()
        yield tag_loader.load_item()
