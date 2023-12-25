import psycopg2

class PsqlDB:
    def __init__(self, dbname, user):
        self.conn = psycopg2.connect("dbname={} user={}".format(dbname, user))
        self.cur = self.conn.cursor()
    
    def get_data_from_table(self, table, column, time_from, time_to):
        query = """
        SELECT SUM({column}) 
        FROM {table}
        WHERE stamp_inserted >= %s AND stamp_inserted <= %s;
        """.format(column=column, table=table)
        self.cur.execute(query, (time_from, time_to))
        result = self.cur.fetchone()
        return result[0] if result else 0

    def get_bytes(self, time_from, time_to):
        data = self.get_data_from_table('acct', 'bytes', time_from, time_to)
        return data if data != None else 0

    def get_packets(self, time_from, time_to):
        data = self.get_data_from_table('acct', 'packets', time_from, time_to)
        return data if data != None else 0

    def close_conn(self):
        self.cur.close()
        self.conn.close()


