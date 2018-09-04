import sqlite3


class Table:
    def __init__(self, name=None):
        import os
        self.con = sqlite3.connect(os.getcwd() + "/daily.db", timeout=10)
        self.cursor = self.con.cursor()
        self.name = name or 'daily'
        print("%s 테이블을 호출합니다" % self.name)

    def load_table(self):
        return self.cursor.execute("SELECT * FROM {tn}".format(tn=self.name))

    # def delete_table(self):
    #     self.cursor.execute("DROP TABLE {tn}".format(tn=self.name))
    #     print("%s 테이블을 삭제하였습니다." % self.name)

    def get_last_date(self, code):
        import datetime
        self.cursor.execute("SELECT * FROM {tn} WHERE code='{code}' ORDER BY id DESC LIMIT 1"
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
        print("sql connect closed")

    def create_table(self):
        self.cursor.execute('PRAGMA encoding="UTF-8"')
        try:
            self.cursor.execute(
                """CREATE TABLE {tn}(id INTEGER PRIMARY KEY, date date, code char(6), name char(30), open real, high real,
                low real, close real, diff real, volume real)""".format(tn=self.name))
        except sqlite3.OperationalError:
            print("{tn} 테이블이 이미 존재합니다".format(tn=self.name))

    def insert_row(self, value):
        self.cursor.execute("""
                            INSERT INTO {tn}(date, code, name, open, high, low, close, diff, volume) 
                            VALUES (?,?,?,?,?,?,?,?,?)""".format(tn=self.name), value)

    def get_rows(self, date, values):
        query = "SELECT {val} FROM {tn} WHERE {tn}.date = {date}" \
            .format(val=', '.join(values), tn=self.name, date=date)
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def new_col(self, col_name):
        # 에러처리 필요 try:
        self.cursor.execute('ALTER TABLE %s ADD COLUMN %s' % (self.name, col_name))

    def __delete__(self):
        self.con.close()
        print("sql connect closed")
