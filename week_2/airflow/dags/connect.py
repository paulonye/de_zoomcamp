from sqlalchemy import create_engine
import os

user = os.environ.get('PGUSER', 'main')
password = os.environ.get('PGPASS', 'root')
port = 5432
host = os.environ.get('HOST', '*********')
db = os.environ.get('DB', 'ny_taxi')

def get_connection():

    postgres_url = f'postgresql://{user}:{password}@{host}:{port}/{db}'

    engine  = create_engine(postgres_url)

    return engine


if __name__ == '__main__':
    con = get_connection()
    print('Connecton Successful')
    con.connect().close()