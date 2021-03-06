#-*- coding: utf-8 -*-

import sqlite3


class Table:
    def __init__(self, name=None):
        import os
        self.con = sqlite3.connect(os.getcwd() + "/daily.db", timeout=10)
        self.cursor = self.con.cursor()
        self.name = name or 'daily'
        print("SQLITE3: open %s table" % self.name)

    def load_table(self):
        return self.cursor.execute("SELECT * FROM {tn}".format(tn=self.name))

    # def delete_table(self):
    #     self.cursor.execute("DROP TABLE {tn}".format(tn=self.name))

    def get_last_date(self, code):
        import datetime
        self.cursor.execute("SELECT * FROM {tn} WHERE code='{code}' ORDER BY {tn}.date DESC LIMIT 1"
                            .format(tn=self.name, code=code))
        try:
            return datetime.datetime.strptime(self.cursor.fetchone()[1], '%Y-%m-%d %H:%M:%S')  # date column
        except TypeError:
            # print("Couldn't Find Last Date")
            return None

    def commit(self):
        self.con.commit()

    def close(self):
        self.con.close()
        print("SQLITE3: sql connect closed")

    def create_table(self):
        self.cursor.execute('PRAGMA encoding="UTF-8"')
        try:
            self.cursor.execute(
                """CREATE TABLE {tn}(id INTEGER PRIMARY KEY, date date, code char(6), name char(30), open real, high real,
                low real, close real, diff real, volume real)""".format(tn=self.name))
        except sqlite3.OperationalError:
            print("SQLITE3: {tn} already exists".format(tn=self.name))

    def insert_row(self, value):
        self.cursor.execute("""
                            INSERT INTO {tn}(date, code, name, open, high, low, close, diff, volume) 
                            VALUES (?,?,?,?,?,?,?,?,?)""".format(tn=self.name), value)

    def get_rows(self, code, values):
        query = "SELECT {val} FROM {tn} WHERE {tn}.code = '{code}' ORDER BY {tn}.date" \
            .format(val=', '.join(values), tn=self.name, code=code)
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def new_col(self, col_name):
        # 에러처리 필요 try:
        self.cursor.execute('ALTER TABLE %s ADD COLUMN %s' % (self.name, col_name))

    def __delete__(self):
        self.close()


class TableSecondary(Table):
    def __init__(self, name):
        import os
        self.con = sqlite3.connect(os.getcwd() + "/daily_secondary.db", timeout=10)
        self.cursor = self.con.cursor()
        self.name = name
        print("SQLITE3: open %s table" % self.name)

    def create_table(self, query):
        self.cursor.execute('PRAGMA encoding="UTF-8"')
        # query = """CREATE TABLE {tn}(id INTEGER PRIMARY KEY, date date, code char(6), name char(30), open real, high real,
        #         low real, close real, diff real, volume real)""".format(tn=self.name)
        try:
            self.cursor.execute(query)
        except sqlite3.OperationalError:
            print("SQLITE3: {tn} already exists".format(tn=self.name))

    def get_columns(self):
        self.cursor.execute("PRAGMA table_info({tn})".format(tn=self.name))

    def insert_row(self, value, query):
        self.cursor.execute(query, value)
