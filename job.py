import scraping.scraping_from_naver as sc
import scraping.codes
from db.table import Table
from multiprocessing import Pool

codes = scraping.codes.codes()

# multiprocess
# pool = Pool(processes=4)
# for x in pool.map(get_content, list_clients):
#     result.append(x)

for corp in codes.itertuples():
    code, name = corp['corp_code'], corp['corp_name']

    data = sc.getData(code)
    data = sc.preprocess(data, code, name)

    table = Table()
    data.to_sql(name=table.name, con=table.con, if_exists='append', index=False)




table.commit()
table.close()