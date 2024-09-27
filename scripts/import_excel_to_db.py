import os
import pandas as pd
import psycopg2
from psycopg2 import sql
from pathlib import Path
from psycopg2.extras import execute_values


def get_excel_files(directory):
    return [str(file) for file in Path(directory).rglob("*.xlsx")]

def create_table():
    pass
    # -- Table: public.citizens

    # DROP TABLE IF EXISTS public.citizens;

    # CREATE TABLE IF NOT EXISTS public.citizens
    # (
    #     id BIGSERIAL NOT NULL,
    #     s1 character varying COLLATE pg_catalog."default",
    #     sta_time character varying COLLATE pg_catalog."default",
    #     status character varying COLLATE pg_catalog."default",
    #     code_number character varying COLLATE pg_catalog."default",
    #     birthdate character varying COLLATE pg_catalog."default",
    #     gender character varying COLLATE pg_catalog."default",
    #     uid character varying COLLATE pg_catalog."default",
    #     uname character varying COLLATE pg_catalog."default",
    #     phone character varying COLLATE pg_catalog."default",
    #     address character varying COLLATE pg_catalog."default",
    #     c1 character varying COLLATE pg_catalog."default",
    #     c2 character varying COLLATE pg_catalog."default",
    #     CONSTRAINT citizens_pkey PRIMARY KEY (id)
    # )

    # TABLESPACE pg_default;

    # ALTER TABLE IF EXISTS public.citizens
    #     OWNER to postgres;
    
def insert_data(cur, table_name, df):
    insert_query = sql.SQL(f"INSERT INTO {table_name} (s1, sta_time, status, code_number, birthdate, gender, uid, uname, phone, address, c1, c2) VALUES %s").format(
        sql.Identifier(table_name)
    )
    values = [tuple(x) for x in df.values]
    execute_values(cur, insert_query, values)

    # columns = df.columns
    # insert_query = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
    #     sql.Identifier(table_name),
    #     sql.SQL(", ").join(map(sql.Identifier, columns))
    # )
    # values = [tuple(x) for x in df.values]
    # psycopg2.extras.execute_values(cur, insert_query, values)
    # cur.execute(f"INSERT INTO {table_name} 
    #             (date, open, high, low, close) 
    #             VALUES (%s, %s, %s, %s, %s)",
    #             values)

def load_excel_to_postgresql(directory, conn, chunk_size=1000):
    files = get_excel_files(directory)

    print(f"files = {len(files)}")

    for i, file in enumerate(files):
        print(f"i = {i}, file = {file}")
        try:
            xls = pd.ExcelFile(file)
            for sheet_name in xls.sheet_names:
                df = pd.read_excel(xls, sheet_name=sheet_name, usecols='A:L')
                print(len(df))
                # chunk_range = range(0, len(df), chunk_size)

                # for i in chunk_range:
                #     chunk_range.count
                #     df_chunk = df[i: i + chunk_size]
                for df_chunk in [df[i: i + chunk_size] for i in range(0, len(df), chunk_size)]:
                    # print(len(df_chunk))
                    with conn.cursor() as cur:
                        insert_data(cur, "citizens", df_chunk)
                        conn.commit()
        except Exception as e:
            print(e)


def chunk_insert(df, chunk_size):
    print(len(df))
    for df_chunk in [df[i: i + chunk_size] for i in range(0, len(df), chunk_size)]:
        print(len(df_chunk))
        with conn.cursor() as cur:
            insert_data(cur, "citizens", df_chunk)
            conn.commit()


# Connection parameters
conn = psycopg2.connect(
    host="127.0.0.1",
    port="5432",
    database="postgres",
    user="postgres",
    password="1234561"
)


# Load data from Excel files to PostgreSQL
directory_path = '/excel_path'
load_excel_to_postgresql(directory_path, conn, 1000)

# Close the connection
conn.close()

