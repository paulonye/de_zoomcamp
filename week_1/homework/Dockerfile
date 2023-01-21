FROM python:3.8

COPY requirements.txt home/app/requirements.txt

RUN pip install --no-cache-dir -r /home/app/requirements.txt

COPY ingest.py /home/app/ingest.py
COPY connect.py /home/app/connect.py
COPY dbcredentials.txt /home/app/dbcredentials.txt
WORKDIR /home/app

ENTRYPOINT ["python", "/home/app/ingest.py"]
CMD ["--url", "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz", \
    "--filetype", "csv.gz", "--tablename", "maintable"]
