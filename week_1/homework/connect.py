from sqlalchemy import create_engine


cred = []
with open('dbcredentials.txt', 'r') as file:
    for line in file:
        cred.append(line.strip())

user = cred[0]
password = cred[1]
port = 5432
host = cred[2]
db = cred[3]

def get_connection():

    postgres_url = f'postgresql://{user}:{password}@{host}:{port}/{db}'

    engine  = create_engine(postgres_url)

    return engine


if __name__ == '__main__':
    con = get_connection()
    print('Connecton Successful')
    con.connect().close()