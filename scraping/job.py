import scraping_from_naver as sc
import codes


codes = codes.codes()
for corp in codes.itertuples():
    code, name = corp['code'], corp['name']

    got_data = sc.getData(code)
    data = sc.preprocess(got_data, code, name)

    insert(data)