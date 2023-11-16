import psycopg2
from scrapy.exceptions import DropItem

from kaimono.settings import DATABASE_SETTINGS


class PSQLPipeline:
    SQL_INSERT = "INSERT TABLE {table_name} ({fields}) VALUES ({values})"
    
    def __init__(self):
        self.conn = None
        self.cur = None
        self.table_name = None
    
    def open_spider(self, spider):
        self.conn = psycopg2.connect(**DATABASE_SETTINGS)
        self.cur = self.conn.cursor()
        self.table_name = getattr(spider, 'table_name')
    
    def close_spider(self, spider):
        self.cur.close()
        self.conn.close()
        
    def process_item(self, item, spider):
        try:
            fields = item.keys()
            sql = self.SQL_INSERT.format(
                table_name=self.table_name,
                fields=fields,
                values=', '.join(['%s' for _ in fields])
            )
            self.cur.exectute(sql, tuple(item.values()))
        except Exception as e:
            # Rollback if an error occurs
            self.conn.rollback()
            raise DropItem(f"Failed to process item: {e}")
        else:
            # Commit changes if successful
            self.conn.commit()
            return item
            