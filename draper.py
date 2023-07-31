import os
import json
import time
import requests
import time
from dotenv import load_dotenv
import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker

load_dotenv()
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
API_SECRET = os.getenv('SHOPIFY_API_SECRET')

engine = sqlalchemy.create_engine(f"mysql+mysqlconnector://{DB_USER}:{DB_PASS}@localhost/catalog")
Session = sessionmaker(bind=engine)

def current_time():
    return time.strftime("%Y-%m-%d %H:%M:%S")

def download_csv(url, filename):
    print(current_time(), 'downloading csv...')
    response = requests.get(url)
    print(current_time(), 'finished download')

    with open(filename, 'wb') as f:
        f.write(response.content)

def update_database(filename):
    df = pd.read_csv(filename).iloc[:, :-1] # remove last column
    df.drop('Catalogue Identifier', axis=1, inplace=True)
    df.columns = df.columns.str.lower().str.replace(' ', '_') # fix column names

    metadata = sqlalchemy.MetaData()
    vendor_draper = sqlalchemy.Table(
        'vendor_draper', metadata,
        sqlalchemy.Column('bar_code', sqlalchemy.BigInteger),
        sqlalchemy.Column('stock_no', sqlalchemy.Integer),
        sqlalchemy.Column('part_no', sqlalchemy.String(64)),
        sqlalchemy.Column('item_description', sqlalchemy.String(1024)),
        sqlalchemy.Column('price', sqlalchemy.Float),
        sqlalchemy.Column('unit_of_sale', sqlalchemy.Integer),
        sqlalchemy.Column('discount_code', sqlalchemy.String(1)),
        sqlalchemy.Column('dealer_code', sqlalchemy.String(1)),
        sqlalchemy.Column('catalogue_page_no', sqlalchemy.Integer),
        sqlalchemy.Column('catalogue_seq_no', sqlalchemy.Integer),
        sqlalchemy.Column('status', sqlalchemy.String(1)),
        sqlalchemy.Column('available', sqlalchemy.String(1))
    )
    metadata.create_all(engine, checkfirst=True)

    with Session() as session:
        df.to_sql('vendor_draper', con=engine, if_exists='append', index=False)
    return update_catalog(vendor_draper, df)

def check_product_data(row, field_name, product_within_db, update_text, values):
    if pd.notnull(row[field_name]) and row[field_name] != product_within_db[field_name]:
        update_text.append(f"{field_name.upper()} {product_within_db[field_name]} -> {row[field_name]}")
        values[field_name] = row[field_name]

def update_catalog(vendor_draper, df):
    print(current_time(), 'start updating catalog...')
    with engine.connect() as connection:
        # Make a single query to get all products
        s = sqlalchemy.select(vendor_draper)
        result = connection.execute(s)

        # Save products to a dictionary for faster lookup
        db_products = {row['bar_code']: row for row in result.mappings()}

        for index, row in df.iterrows():
            barcode = row['bar_code']
            
            # Use dict for faster lookup, no db query is made in this loop
            product_within_db = db_products.get(barcode)

            update_text = []
            values = {}

            if product_within_db:
                stmt = sqlalchemy.update(vendor_draper)
                for field_name in df.columns:
                    check_product_data(row, field_name, product_within_db, update_text, values)
                if values:
                    print(f"\nUPDATED PRODUCT: {row['item_description']} ({row['bar_code']})")
                    for text in update_text:
                        print(text)
                    stmt = stmt.where(vendor_draper.c.bar_code == barcode).values(**values)
                    connection.execute(stmt)
            else:
                print(f"\nADDING NEW PRODUCT: {row['item_description']} ({row['bar_code']})")
                stmt = sqlalchemy.insert(vendor_draper).values(row)
                connection.execute(stmt)
    print(current_time(), '\nfinished updating catalog')

# Function to update Shopify product using the Shopify API
def update_shopify_product(product_code, new_stock_value):
    url = f"https://boffer-3019.myshopify.com/admin/api/2023-07/products/{product_code}.json"
    payload = json.dumps({
        "product": {
            "id": product_code,
            "variants": [
                {
                    "inventory_quantity": new_stock_value
                }
            ]
        }
    })
    headers = {
        'X-Shopify-Access-Token': {API_SECRET},
        'Content-Type': 'application/json'
    }
    response = requests.request("PUT", url, headers=headers, data=payload)
    print(response.text)

def main():
    url = "https://b2b.drapertools.com/products/pricefiles/draper_list_prices_uk.csv"
    discord_webhook_url = "YOUR_DISCORD_WEBHOOK_URL_HERE"

    while True:
        filename = f"draper-{time.strftime('%Y%m%d-%H%M%S')}.csv"

        download_csv(url, filename)
        update_database(filename)
        
        print(current_time(), 'waiting 20 minutes...')
        time.sleep(1200)

if __name__ == "__main__":
    main()