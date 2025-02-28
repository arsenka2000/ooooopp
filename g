#pl + fa
import psycopg2,csv
conntion = psycopg2.connect(database="tourism2", user="univer", password="univer", port=5432, host="127.0.0.1")
cur = conntion.cursor()
with open("План.csv", "r", encoding="utf-8") as file:
    reader = csv.reader(file)
    data = list(reader)
    data.pop(0)
sql_create_table_plan = """
CREATE TABLE public."план" (
);"""
cur.execute(sql_create_table_plan)
conntion.commit()
for i in range(len(data)):
    sql_insert_plan = f"""
    INSERT INTO public."план" (
        );"""
    cur.execute(sql_insert_plan)
    conntion.commit()
conntion.close()

text_in_cp1251 = open('Общая статистика.csv', 'rb').read()
text_in_unicode = text_in_cp1251.decode('cp1251')
text_in_utf8 = text_in_unicode.encode('utf8')
open('stat.csv', 'wb').write(text_in_utf8)

import pandas as pd
df_stat = pd.read_csv("stat.csv",delimiter=";")
df_stat.dropna(inplace=True)
df_stat.to_csv("stat2.csv", index=False)

#st
import csv
import psycopg2
with open("stat2.csv", "r", encoding="utf-8") as file:
    reader = csv.reader(file)
    data = list(reader)
    data.pop(0)

conn = psycopg2.connect(database="tourism2", user="univer", password="univer", port=5432, host="127.0.0.1")
cur = conn.cursor()

sql_create_ststic = """
CREATE TABLE public."общая статистика" (
    );"""
cur.execute(sql_create_ststic)
conn.commit()

for i in range(len(data)):
    sql_insert_static = f"""
    INSERT INTO public."общая статистика" (
   ) 
VALUES (
    );"""
    cur.execute(sql_insert_static)
conn.commit()
conn.close()

#tr

#ts
import psycopg2
nRow = 0
th = ['address','name_ru','rating','rubrics','text']
lth = [7, 7, 6, 7, 4]
aa = ['','','','','']
Connection = psycopg2.connect(user = "univer", password = "univer", host = "localhost", port = "5432", database = 'tskv')
CurrentCursor =  Connection.cursor()
sql_create_table = """
CREATE TABLE punlic.tskv (
    address text,
    name_ru text,
    rating integer,
    rubrics text,
    text text
);"""
CurrentCursor.execute(sql_create_table)
Connection.commit()
with open("geo-reviews-dataset-2023.tskv", "r") as inp:
    i = 0    
    for line in inp:
        line = line.replace("'", '"')
        arr = line.split("\t")
        j2 = 0
        for j1 in range(0, 5):
            ss = arr[j2]
            a = ss[0:lth[j1]+1]
            if a == (th[j1]+'='):
                aa[j1] = ss[lth[j1]+1:]
                j2 += 1
            else:
                aa[j1] = ''
            j1 += 1
        i+=1
        r = aa[2]
        c1 = r[-1]
        if c1 == '.': r = r[:-1]
        aa[0] = aa[0][0:99]
        aa[1] = aa[1][0:99]
        aa[3] = aa[3][0:99]
        aa[4] = aa[4][0:999]
        SQL = f"INSERT INTO public.tskv2(address, name_ru, rating, rubrics, text) VALUES ('{aa[0]}', '{aa[1]}', {r}, '{aa[3]}', '{aa[4]}');"
        CurrentCursor.execute(SQL)
        Connection.commit()
        #if i == 1000: break
        pass
print("end\n")
