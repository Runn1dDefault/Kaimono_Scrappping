from collections import OrderedDict
from dataclasses import dataclass
from typing import Any

import psycopg2
from psycopg2.extras import execute_values

from kaimono.items import PSQLItemMeta, ProductCategoryItem
from kaimono.settings import DATABASE_SETTINGS


@dataclass
class DistributedData:
    fields: list = None
    to_update: list = None
    to_create: list = None


class PSQLPipeline:
    SQL_INSERT = "INSERT INTO {table_name} ({fields}) VALUES %s"
    SQL_UPDATE = "UPDATE {table_name} SET {field_sets} FROM (VALUES %s) as tmp ({fields}) WHERE {condition}"
    SQL_EXISTS = "SELECT EXISTS (SELECT 1 FROM {table_name} WHERE {condition})"
    SQL_SELECT_ALL = "SELECT {fields} FROM {table_name}"
    SQL_SELECT_ONE = "SELECT {fields} FROM {table_name} WHERE {match_field} = %s"
    SQL_SELECT_CONDITION = "SELECT {fields} FROM {table_name} WHERE {condition}"
    
    def __init__(self):
        self.conn = None
        self.cur = None
        self.logger = None

    def open_spider(self, spider):
        self.conn = psycopg2.connect(**DATABASE_SETTINGS)
        self.cur = self.conn.cursor()
        self.logger = spider.logger

    def close_spider(self, spider):
        spider.logger.debug(f'{self.__class__.__name__} close...')
        self.cur.close()
        self.conn.close()

    def process_item(self, item, spider):
        if not hasattr(item, 'Meta') or not issubclass(getattr(item, 'Meta'), PSQLItemMeta):
            spider.logger.debug(f'{self.__class__.__name__} passed item...')
            return item

        db_meta = item.Meta
        distributed_data = self.distribute_data(item)
        if distributed_data.to_create:
            self.create(
                table_name=db_meta.db_table,
                fields=distributed_data.fields,
                values=distributed_data.to_create
            )
        if distributed_data.to_update:
            self.update(
                table_name=db_meta.db_table,
                fields=distributed_data.fields,
                match_fields=db_meta.match_fields,
                values=distributed_data.to_update
            )
        return item

    def distribute_data(self, item) -> DistributedData:
        db_meta = item.Meta
        distributed_data = DistributedData(fields=[], to_create=[], to_update=[])
        collected_data, checks_to_exists = [], OrderedDict()

        for field in db_meta.fields:
            values = item.get(field)

            if values is None:
                continue

            if field in db_meta.substitutions:
                substitution = db_meta.substitutions[field]

                # replacing values by matched ids
                values = self.match_ids(
                    table_name=substitution.table or db_meta.db_table,
                    match_field=substitution.field,
                    values=values
                )
                if not values:
                    continue

            distributed_data.fields.append(field)
            collected_data.append(values)

            if field in db_meta.match_fields:
                checks_to_exists[field] = values

        collected_data = list(zip(*collected_data))

        if not db_meta.match_fields:
            distributed_data.to_create.extend(collected_data)
            return distributed_data

        # {field: [value, ...], field_2: [value_2, ...], ...}
        processed_matches = set()

        for index, matches in enumerate(tuple(zip(*checks_to_exists.values()))):
            if matches in processed_matches:
                continue

            processed_matches.add(matches)
            data = collected_data[index]
            checks = dict(zip(checks_to_exists.keys(), matches))

            row_exists = self.check_exists(db_meta.db_table, **checks)
            if row_exists:
                if item.Meta.do_update is False:
                    continue

                distributed_data.to_update.append(data)
            else:
                distributed_data.to_create.append(data)
        return distributed_data

    def match_ids(
        self,
        table_name: str,
        match_field: str,
        values: list[str | int],
        pk_field: str = "id"
    ) -> list[str | int]:
        try:
            sql = self.SQL_SELECT_CONDITION.format(
                table_name=table_name,
                fields=f"{pk_field}, {match_field}",
                condition=f"{match_field} IN %s"
            )
            self.cur.execute(sql, (tuple(values),))
            result = self.cur.fetchall()
        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Failed to process item on match ids {e.__class__.__name__}: {e}")
            raise e
        else:
            if not result:
                return []

            ids = [_id for value in values for _id, match in result if value == match]
            assert len(ids) == len(values)
            return ids

    def check_exists(self, table_name: str, **matches) -> bool:
        try:
            condition = " AND ".join([f"{field} = '{value}'" for field, value in matches.items()])
            sql = self.SQL_EXISTS.format(
                fields=", ".join(matches.keys()),
                table_name=table_name,
                condition=condition
            )
            self.cur.execute(sql)
            return bool(self.cur.fetchone()[0])
        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Failed to process item on check to exists: {e}")

    def create(self, table_name, fields: tuple[str], values: list[tuple[Any | None]]) -> None:
        try:
            sql = self.SQL_INSERT.format(
                table_name=table_name,
                fields=", ".join(fields)
            )
            execute_values(self.cur, sql, values)
            self.logger.info("Saved to DB: %s" % len(values))
        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Failed to process item on creating: {e}")
            raise e
        else:
            self.conn.commit()

    def update(
        self,
        table_name: str,
        fields: tuple[str],
        match_fields: tuple[str],
        values: list[tuple[Any | None]]
    ) -> None:
        conditions = ' AND '.join(
            [f"{table_name}.{match_field} = tmp.{match_field}" for match_field in match_fields]
        )
        field_sets = ', '.join([f'{field} = tmp.{field}' for field in fields])
        try:
            sql = self.SQL_UPDATE.format(
                table_name=table_name,
                field_sets=field_sets,
                fields=", ".join(fields),
                condition=conditions
            )
            execute_values(self.cur, sql, values)
            self.logger.info("Updated in DB: %s" % len(values))
        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Failed to process item on updating: {e}")
            raise e
        else:
            self.conn.commit()
