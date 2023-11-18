from typing import Iterable


def build_uniqlo_id(site_id: int | str) -> str:
    return f"uniqlo_{site_id}"


def build_rakuten_id(site_id: int | str) -> str:
    return f"rakuten_{site_id}"


def get_site_id_from_db_id(_id: str) -> str:
    return _id.split('_')[-1]


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


def delete_products_by_ids(conn, product_ids: Iterable[str]):
    sql = "DELETE FROM products_product WHERE id IN %s"

    try:
        with conn.cursor() as cur:
            cur.execute(sql, (tuple(product_ids),))
            cur.commit()
    except Exception as e:
        conn.rollback()
        raise Exception(f"Failed to delete products by ids: {e}")


def site_tags(conn, site):
    sql = f"SELECT id FROM products_tag WHERE id like '{site}%'"
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchall()
    except Exception as e:
        conn.rollback()
        raise Exception(f"Failed to get tag ids for site {site}: {e}")
