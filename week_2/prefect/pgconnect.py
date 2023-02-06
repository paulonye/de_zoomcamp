from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
load_dotenv()


def get_connection():

    user = os.environ.get('PGUSER')
    password = os.environ.get('PGPASS')
    port = 5432
    host = os.environ.get('HOST')
    db = os.environ.get('DB')

    postgres_url = f'postgresql://{user}:{password}@{host}:{port}/{db}'

    engine  = create_engine(postgres_url)

    return engine

if __name__ == '__main__':
    con = get_connection()
    print('Connecton Successful')
    con.connect().close()