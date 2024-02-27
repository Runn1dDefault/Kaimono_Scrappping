from datetime import datetime
from typing import Iterable


def build_uniqlo_id(site_id: int | str) -> str:
    return f"uniqlo_{site_id}"


def build_rakuten_id(site_id: int | str) -> str:
    return f"rakuten_{site_id}"


def get_site_id_from_db_id(_id: str) -> str:
    return _id.split('_')[-1]


def product_ids_to_check_count(conn, site: str, check_time: datetime) -> int:
    sql = """
        SELECT 
            COUNT(p.id) 
        FROM products_product AS p
        LEFT OUTER JOIN products_product_categories AS pc ON p.id = pc.product_id
        LEFT OUTER JOIN products_category AS c ON pc.category_id = c.id
        WHERE 
            p.id like %s 
            AND p.is_active 
            AND NOT c.deactivated
            AND p.modified_at < %s::timestamp
        """
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (f"{site}%", check_time.strftime("%Y-%m-%d %H:%M:%S")))
            return int(cur.fetchone()[0])
    except Exception as e:
        conn.rollback()
        return 0


def product_ids_to_check(conn, site: str, check_time: datetime, limit: int, offset: int = 0):
    sql = """
    SELECT DISTINCT ON (p.id) p.id FROM products_product AS p
    INNER JOIN products_product_categories AS pc ON p.id = pc.product_id
    INNER JOIN products_category AS c ON pc.category_id = c.id
    WHERE 
        p.id like %s
        AND p.is_active 
        AND NOT c.deactivated
        AND p.modified_at < %s::timestamp
    ORDER BY p.id, p.modified_at ASC
    LIMIT %s OFFSET %s;
    """
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (f"{site}%", check_time.strftime("%Y-%m-%d %H:%M:%S"), limit, offset))
            return cur.fetchall()
    except Exception as e:
        conn.rollback()
        raise e
        # return []


def category_ids_for_scrape(conn, site: str):
    sql = """
    SELECT id FROM products_category 
    WHERE id like %s AND deactivated = false AND level = 1
    """

    try:
        with conn.cursor() as cur:
            cur.execute(sql, (f"{site}%",))
            base_genre_ids = cur.fetchall()
    except Exception as e:
        conn.rollback()
        raise Exception(f"Failed to retrieve genres for scraping: {e}")
    else:
        genre_ids = set()
        for base_genre_id in base_genre_ids:
            last_children = set(get_last_children(conn, base_genre_id, site))
            genre_ids.update(last_children)
        return genre_ids


def get_last_children(conn, current_genre_id, site):
    sql = """
    WITH RECURSIVE last_children(id) AS (
        SELECT id FROM products_category
        WHERE id like %s AND parent_id = %s AND deactivated = false
        AND NOT EXISTS (SELECT 1 FROM products_category AS g WHERE g.parent_id = products_category.id)
        UNION ALL
        SELECT g.id FROM products_category AS g
        INNER JOIN products_category AS lc ON lc.id = g.parent_id
        WHERE g.id LIKE %s
    )
    SELECT id FROM last_children;
    """
    check_id = f"{site}%"

    try:
        with conn.cursor() as cur:
            cur.execute(sql, (check_id, current_genre_id, check_id))
            rows = cur.fetchall()
            last_children = [row[0] for row in rows]

    except Exception as e:
        conn.rollback()
        raise Exception(f"Failed to retrieve last children: {e}")

    return last_children


def get_genres_tree(conn, current_genre_id):
    collected_parents = [current_genre_id]
    sql = "SELECT parent_id FROM products_category WHERE id = %s"

    try:
        with conn.cursor() as cur:
            while True:
                cur.execute(sql, (current_genre_id,))
                parent_id = cur.fetchone()

                if not parent_id:
                    break

                parent_id = parent_id[0]
                collected_parents.append(parent_id)
                current_genre_id = parent_id
    except Exception as e:
        conn.rollback()
        raise Exception(f"Failed to retrieve genres for scraping: {e}")
    return collected_parents


def delete_product_exclude_images(conn, product_id: str, image_urls: list[str]):
    sql = """
    DELETE FROM products_productimage AS img
    WHERE ID IN (
        SELECT id FROM products_productimage
        WHERE product_id = %s AND url NOT IN %s
    )
    """

    try:
        with conn.cursor() as cur:
            cur.execude(sql, (product_id, tuple(image_urls)))
            cur.commit()
    except Exception:
        conn.rollback()


def tag_exists(conn, tag_id):
    sql = "SELECT EXISTS (SELECT 1 FROM products_tag WHERE id = %s)"

    try:
        with conn.cursor() as cur:
            cur.execute(sql, (tag_id,))
            return bool(cur.fetchone()[0])
    except Exception as e:
        conn.rollback()
        raise Exception(f"Failed to check tag id to exist {tag_id}: {e}")


def get_product_variation_id(conn, category_id, catch_copy, shop_code):
    sql = """
    SELECT p.id FROM products_product AS p
    JOIN products_product_categories AS pc ON p.id = pc.product_id
    JOIN products_category AS c ON pc.category_id = c.id
    WHERE 
        c.id = %s 
        AND p.catch_copy = %s 
        AND p.shop_code = %s;
    """
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (category_id, catch_copy, shop_code))
            result = cur.fetchone()
            return result[0] if result is not None else None
    except Exception as e:
        conn.rollback()
        raise Exception(f"Failed to get product variation: {e}")

