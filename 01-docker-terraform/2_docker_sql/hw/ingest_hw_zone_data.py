import os
import argparse
import pandas as pd
from sqlalchemy import create_engine
from time import time


def download_data(url: str) -> None:
    csv_name = os.path.basename(url)

    os.system(f"wget {url} -O {csv_name}")

    return None


def load_data_iter(
    path: str = "data/green_tripdata_2015-09.csv", chunk_size=10000
) -> pd.DataFrame:
    df = pd.read_csv(path, iterator=True, chunksize=chunk_size)

    return df


def define_engine(
    host: str = "localhost",
    port: str = "5432",
    user: str = "root",
    password: str = "root",
    db: str = "ny_taxi",
) -> None:
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

    return engine


def clean_timestamps(df: pd.DataFrame) -> pd.DataFrame:
    df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
    df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])

    return df


def write_to_db(
    df_iter: pd.DataFrame, table: str = "green_tripdata_2015_09", engine: str = None
) -> None:
    # grab the first chunk of the data
    df = next(df_iter)
    # clean_timestamps(df)
    # create or replace the table
    df.head(n=0).to_sql(name=table, con=engine, if_exists="replace")

    # insert the first chunk
    t_start = time()
    df.to_sql(name=table, con=engine, if_exists="append")
    t_end = time()

    # print the time it took to insert the first chunk
    print("inserted the first chunk, took %.3f second" % (t_end - t_start))

    while True:
        try:
            t_start = time()

            df = next(df_iter)

            # clean_timestamps(df)

            df.to_sql(name=table, con=engine, if_exists="append")

            t_end = time()

            print("inserted another chunk, took %.3f second" % (t_end - t_start))

        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break


def main_flow(params) -> None:
    # extract the parameters from the command line arguments
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table = params.table
    url = params.url

    # download the data from the internet
    download_data(url)

    # extract the file name from the url
    path = os.path.basename(url)

    # load the csv file as a pandas dataframe iterable
    df_iter = load_data_iter(path)

    # create the postgres connection engine
    engine = define_engine(host, port, user, password, db)

    # write the data to the database
    write_to_db(df_iter, table, engine)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest data into postgres database")
    parser.add_argument("--user", type=str, help="postgres user")
    parser.add_argument("--password", type=str, help="postgres password")
    parser.add_argument("--host", type=str, help="postgres host")
    parser.add_argument("--port", type=int, help="postgres port")
    parser.add_argument("--db", type=str, help="postgres db")
    parser.add_argument("--table", type=str, help="postgres table")
    parser.add_argument("--url", type=str, help="url of the data to ingest")

    args = parser.parse_args()

    main_flow(args)
