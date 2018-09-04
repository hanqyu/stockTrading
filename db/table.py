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

    def get_last_row(self):
        self.cursor.execute("SELECT * FROM {tn} ORDER BY id DESC LIMIT 1".format(tn=self.name))
        return self.cursor.fetchone()

    def commit(self):
        self.con.commit()

    def close(self):
        self.con.close()
        print("sql connect closed")

    def create_table(self):
        try:
            self.cursor.execute(
                """CREATE TABLE {tn}(id INTEGER PRIMARY KEY, Date date, Code int, Name char(30), Open real, High real,
                Low real, Close real, Diff real, Volume real)""".format(tn=self.name))
        except sqlite3.OperationalError:
            print("{tn} 테이블이 이미 존재합니다".format(tn=self.name))

    def insert_row(self, value):
        self.cursor.execute("""
                            INSERT INTO {tn}(Date, Code, Name, Open, High, Low, Close, Diff, Volume) 
                            VALUES (?,?,?,?,?,?,?,?,?)""".format(tn=self.name), value)

    def get_rows(self, date, values):
        query = "SELECT {val} FROM {tn} WHERE {tn}.Date = {date}" \
            .format(val=', '.join(values), tn=self.name, date=date)
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def new_col(self, col_name):
        # 에러처리 필요 try:
        self.cursor.execute('ALTER TABLE %s ADD COLUMN %s' % (self.name, col_name))

    def __delete__(self):
        self.con.close()
        print("sql connect closed")