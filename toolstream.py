import os
from dotenv import load_dotenv
import requests
import pandas as pd
import time
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

load_dotenv()
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')

API_SECRET = os.getenv('SHOPIFY_API_SECRET')
AUTH_TOKEN = os.getenv('SHOPIFY_AUTH_TOKEN')

engine = create_engine(f"mysql+mysqlconnector://{DB_USER}:{DB_PASS}@localhost/catalog")
Session = sessionmaker(bind=engine)

def current_time():
    return time.strftime("%Y-%m-%d %H:%M:%S")

def download_csv(url, filename):
    print(current_time(), 'downloading csv...')
    response = requests.get(url)
    print(current_time(), 'finished download')
    
    with open(filename, 'wb') as f:
        f.write(response.content)

def create_database(filename):
    print(current_time(), 'uploading csv to database...')
    df = pd.read_csv(filename)
    
    with Session() as session:
        df.to_sql('vendor_toolstream', con=engine, if_exists='replace', index=False)
    print(current_time(), 'finished updating database')

def main():
    url = f"https://www.toolstream.com/api/v1/GetProducts?&token={AUTH_TOKEN}&format=csv&language=en-GB"

    while True:
        filename = f"toolstream-{time.strftime('%Y%m%d-%H%M%S')}.csv"

        download_csv(url, filename)
        create_database(filename)

        print(current_time(), 'waiting 20 minutes...')
        time.sleep(1200)

if __name__ == "__main__":
    main()