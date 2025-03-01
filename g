#pl + f
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


