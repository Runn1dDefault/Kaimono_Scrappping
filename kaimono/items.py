from dataclasses import dataclass

import scrapy


@dataclass
class Substitution:
    field: str
    table: str = None


class PSQLItemMeta:
    db_table: str = None
    fields: list[str] | tuple[str] = None
    match_fields: tuple[str] = ()  # for updating
    substitutions: dict[str, Substitution] = {}
    do_update: bool = True

    def __init__(self):
        assert self.db_table and self.fields


class CategoryItem(scrapy.Item):
    id = scrapy.Field()
    name = scrapy.Field()
    parent_id = scrapy.Field()
    level = scrapy.Field(serializer=int)

    class Meta(PSQLItemMeta):
        db_table = "products_category"
        fields = ("id", "name", "parent_id", "level")
        match_fields = ("id",)


class ProductItem(scrapy.Item):
    id = scrapy.Field()
    name = scrapy.Field()
    description = scrapy.Field()
    site_price = scrapy.Field()
    site_avg_rating = scrapy.Field()
    site_reviews_count = scrapy.Field()
    product_url = scrapy.Field()

    class Meta(PSQLItemMeta):
        db_table = "products_product"
        fields = ("id", "name", "description", "site_price", "site_avg_rating", "site_reviews_count", "product_url")
        match_fields = ("id",)


class ProductCategoryItem(scrapy.Item):
    product_id = scrapy.Field()
    category_id = scrapy.Field()

    class Meta(PSQLItemMeta):
        db_table = "products_product_categories"
        fields = ("product_id", "category_id")
        match_fields = ("product_id", "category_id")
        do_update = False


class TagItem(scrapy.Item):
    id = scrapy.Field()
    name = scrapy.Field()
    group_id = scrapy.Field()

    class Meta(PSQLItemMeta):
        db_table = "products_tag"
        fields = ("id", "name", "group_id")
        match_fields = ("id",)


class ProductTagItem(scrapy.Item):
    product_id = scrapy.Field()
    tag_id = scrapy.Field()

    class Meta(PSQLItemMeta):
        db_table = "products_product_tags"
        fields = ("product_id", "tag_id")
        match_fields = ("product_id", "tag_id")
        do_update = False


def remove_url_query_param(url: str):
    return url.split('?')[0]


class ProductImageItem(scrapy.Item):
    product_id = scrapy.Field()
    url = scrapy.Field(serializer=remove_url_query_param)

    class Meta(PSQLItemMeta):
        db_table = "products_productimage"
        fields = ("product_id", "url")
        match_fields = ("product_id", "url")


class ProductQuantityItem(scrapy.Item):
    product_id = scrapy.Field()
    color = scrapy.Field()
    color_image_url = scrapy.Field()
    size = scrapy.Field()
    quantity = scrapy.Field()
    site_unit_price = scrapy.Field()
    status_code = scrapy.Field()

    class Meta(PSQLItemMeta):
        db_table = "products_productinventory"
        fields = ("product_id", "color", "color_image_url", "size", "quantity", "site_unit_price", "status_code")
        match_fields = ("product_id", "color", "size")
