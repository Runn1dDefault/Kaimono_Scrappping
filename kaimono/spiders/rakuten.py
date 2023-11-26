import json
import random
from typing import Any, Iterable

import psycopg2
import scrapy
from scrapy import Request
from scrapy.http import Response
from scrapy.loader import ItemLoader

from kaimono.items import CategoryItem, ProductItem, ProductCategoryItem, TagItem, \
    ProductInventoryItem, ProductInventoryTagItem, ProductImageItem
from kaimono.settings import DATABASE_SETTINGS
from kaimono.utils import build_rakuten_id, category_ids_for_scrape, get_genres_tree, get_site_id_from_db_id, \
    tag_exists, get_product_variation_id

RAKUTEN_BASE_URL = "https://app.rakuten.co.jp/"


def random_rakuten_app_id(app_ids):
    return random.choice(app_ids)


class RakutenCategorySpider(scrapy.Spider):
    name = "rakuten_category"
    RAKUTEN_APP_IDS = (
        "1006081949539677212",
        "1032684706123538391",
        "1074124474108574900",
        "1066790171671197123"
    )
    custom_settings = {
        'LOG_LEVEL': 'INFO',
        "DEFAULT_REQUEST_HEADERS": {"Content-Type": "application/json"},
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
        '1052001095841946356',
        '1053826134919859121'
    )

    custom_settings = {
        'LOG_LEVEL': 'INFO',
        "DEFAULT_REQUEST_HEADERS": {
            "Content-Type": "application/json"
        },
        "CONCURRENT_REQUESTS": len(RAKUTEN_APP_IDS) * 5,
    }

    API_URL = ("https://app.rakuten.co.jp/services/api/IchibaItem/Search/20220601"
               "?applicationId={app_id}&formatVersion=2&genreId={genre_id}&tagInformationFlag=1&page={page}")

    TAG_API_URL = RAKUTEN_BASE_URL + ("services/api/IchibaTag/Search/20140222?"
                                      "applicationId={app_id}&formatVersion=2&tagId={tag_id}")
    TRANSLATE_GG_URL = "https://translate.googleapis.com/translate_a/single?client=gtx&dt=t&sl=ja&tl=en&q=%s"

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

        item_loader = ItemLoader(ProductItem())
        item_category_loader = ItemLoader(ProductCategoryItem())
        item_image_loader = ItemLoader(ProductImageItem())
        item_inventory_loader = ItemLoader(ProductInventoryItem())
        inventory_tag_loader = ItemLoader(ProductInventoryTagItem())

        processed_ids = set()
        catch_copies = {}
        request_to_tags = []

        for item in response_data['Items']:
            item_code = item["itemCode"]
            item_id = build_rakuten_id(item_code)
            if item_id in processed_ids:
                continue

            catch_copy, shop_code = item["catchcopy"], item["shopCode"]
            variation_id = (
                    catch_copies.get(shop_code + catch_copy) or
                    get_product_variation_id(self.psql_con, category_id, catch_copy, shop_code)
            )

            if not variation_id:
                item_loader.add_value("id", item_id)
                item_loader.add_value("name", item["itemName"])
                item_loader.add_value("description", item["itemCaption"])
                item_loader.add_value("site_avg_rating", item["reviewAverage"])
                item_loader.add_value("site_reviews_count", item["reviewCount"])
                item_loader.add_value("shop_code", shop_code)
                item_loader.add_value("catch_copy", catch_copy)
                item_loader.add_value("shop_url", item["shopUrl"])
                catch_copies[shop_code + catch_copy] = item_id

                for category_id in categories_tree:
                    item_category_loader.add_value("product_id", item_id)
                    item_category_loader.add_value("category_id", category_id)

                for url in item["mediumImageUrls"] or item["smallImageUrls"]:
                    item_image_loader.add_value("product_id", item_id)
                    item_image_loader.add_value("url", url.split('?')[0])

                variation_id = item_id

            item_inventory_loader.add_value("id", item_id)
            item_inventory_loader.add_value("product_id", variation_id)
            item_inventory_loader.add_value("item_code", item_code)
            item_inventory_loader.add_value("site_price", item['itemPrice'])
            item_inventory_loader.add_value("product_url", item["itemUrl"])
            item_inventory_loader.add_value("name", item["itemName"]),
            item_inventory_loader.add_value("can_choose_tags", False)

            for tag_id in item["tagIds"]:
                db_tag_id = build_rakuten_id(tag_id)

                if not tag_exists(self.psql_con, db_tag_id):
                    request_to_tags.append((item_id, tag_id, db_tag_id))
                    continue

                inventory_tag_loader.add_value("productinventory_id", item_id)
                inventory_tag_loader.add_value("tag_id", db_tag_id)

            processed_ids.add(item_id)

        yield item_loader.load_item()
        yield item_category_loader.load_item()
        yield item_image_loader.load_item()
        yield item_inventory_loader.load_item()

        for item_id, tag_id, db_tag_id in request_to_tags:
            yield scrapy.Request(
                url=self.TAG_API_URL.format(
                    app_id=random_rakuten_app_id(self.RAKUTEN_APP_IDS),
                    tag_id=tag_id
                ),
                callback=self.parse_tag,
                meta={"item_id": item_id, 'tag_id': db_tag_id}
            )

        yield inventory_tag_loader.load_item()

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

        loader = ItemLoader(ProductInventoryTagItem())
        loader.add_value("productinventory_id", response.meta['item_id'])
        loader.add_value("tag_id", response.meta['tag_id'])
        yield loader.load_item()

    def parse_tags(self, tags_data):
        tag_group_loader = ItemLoader(TagItem())
        tag_loader = ItemLoader(TagItem())

        for tag_group_data in tags_data:
            group_id = build_rakuten_id(tag_group_data["tagGroupId"])
            tag_group_loader.add_value("id", group_id)
            group_name = tag_group_data["tagGroupName"]
            if group_name == "ブランド":
                tag_group_loader.add_value("name", "Brand")
            else:
                tag_group_loader.add_value("name", group_name)

            for tag_data in tag_group_data["tags"]:
                tag_id = build_rakuten_id(tag_data["tagId"])
                tag_name = tag_data["tagName"]
                if group_name == "ブランド":
                    yield scrapy.Request(
                        url=self.TRANSLATE_GG_URL % tag_name,
                        callback=self.parse_translated_tag,
                        meta={'original_name': tag_name, 'tag_id': tag_id, 'group_id': group_id}
                    )
                tag_loader.add_value("id", tag_id)
                tag_loader.add_value("name", tag_name)
                tag_loader.add_value("group_id", group_id)

        yield tag_group_loader.load_item()
        yield tag_loader.load_item()

    def parse_translated_tag(self, response: Response):
        translated_tag_name = json.loads(response.body)[0][0][0]
        self.logger.info(f"Tag: {response.meta['original_name']} transtated to {translated_tag_name}")
        tag_loader = ItemLoader(TagItem())
        tag_loader.add_value("id", response.meta['tag_id'])
        tag_loader.add_value("name", translated_tag_name)
        tag_loader.add_value("group_id", response.meta['group_id'])
        yield tag_loader.load_item()
