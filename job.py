import scraping.scraping_from_naver as sc
import scraping.codes
from db.table import Table

codes = scraping.codes.codes()

for corp in codes.itertuples():
    code, name = corp['code'], corp['name']

    got_data = sc.getData(code)
    data = sc.preprocess(got_data, code, name)

    table = Table()
    # df를 row별로, 컬럼 순서 지켜서

    df.to_sql(name=table.name, table.con, if_exists='append', index=False, dtype=)
