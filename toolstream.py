import os
from dotenv import load_dotenv
import requests
import pandas as pd
import time
import sqlalchemy
from sqlalchemy.orm import sessionmaker

load_dotenv()
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
AUTH_TOKEN = os.getenv('TOOLSTREAM_AUTH_TOKEN')

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
    df = pd.read_csv(filename)
    columns_to_keep = ['Product_Code', 'Primary_Description', 'Stock', 'Break_Qty_1', 'Break_Price_1', 'Break_Qty_2', 'Break_Price_2', 'Bulk_Qty', 'Bulk_Price', 'Net_Qty', 'Net_Price', 'Promotional_Price', 'Barcode']
    columns_to_drop = df.columns.difference(columns_to_keep)
    df.drop(columns_to_drop, axis=1, inplace=True)
    df.columns = df.columns.str.lower()
    
    metadata = sqlalchemy.MetaData()
    vendor_toolstream = sqlalchemy.Table(
        'vendor_toolstream', metadata,
        sqlalchemy.Column('product_code', sqlalchemy.String(128)),
        sqlalchemy.Column('primary_description', sqlalchemy.String(128)),
        sqlalchemy.Column('stock', sqlalchemy.Integer),
        sqlalchemy.Column('break_qty_1', sqlalchemy.Integer),
        sqlalchemy.Column('break_price_1', sqlalchemy.Float),
        sqlalchemy.Column('break_qty_2', sqlalchemy.Integer),
        sqlalchemy.Column('break_price_2', sqlalchemy.Float),
        sqlalchemy.Column('bulk_qty', sqlalchemy.Integer),
        sqlalchemy.Column('bulk_price', sqlalchemy.Float),
        sqlalchemy.Column('net_qty', sqlalchemy.Integer),
        sqlalchemy.Column('net_price', sqlalchemy.Float),
        sqlalchemy.Column('promotional_price', sqlalchemy.Float),
        sqlalchemy.Column('barcode', sqlalchemy.BigInteger)
    )
    metadata.create_all(engine, checkfirst=True)
    
    with Session() as session:
        df.to_sql('vendor_toolstream', con=engine, if_exists='append', index=False)
    return update_catalog(vendor_toolstream, df)

def check_product_data(row, field_name, product_within_db, update_text, values):
    if pd.notnull(row[field_name]) and row[field_name] != product_within_db[field_name]:
        update_text.append(f"{field_name.upper()} {product_within_db[field_name]} -> {row[field_name]}")
        values[field_name] = row[field_name]

def update_catalog(vendor_toolstream, df):
    print(current_time(), 'start updating catalog...')
    with engine.connect() as connection:
        # Make a single query to get all products
        s = sqlalchemy.select(vendor_toolstream)
        result = connection.execute(s)

        # Save products to a dictionary for faster lookup
        db_products = {row['product_code']: row for row in result.mappings()}

        for index, row in df.iterrows():
            product_code = row['product_code']
            
            # Use dict for faster lookup, no db query is made in this loop
            product_within_db = db_products.get(product_code)

            update_text = []
            values = {}

            if product_within_db:
                stmt = sqlalchemy.update(vendor_toolstream)
                for field_name in df.columns:
                    check_product_data(row, field_name, product_within_db, update_text, values)
                if values:
                    print(f"\nUPDATED PRODUCT: {row['primary_description']} ({row['product_code']})")
                    for text in update_text:
                        print(text)
                    stmt = stmt.where(vendor_toolstream.c.product_code == product_code).values(**values)
                    connection.execute(stmt)
            else:
                print(f"\nADDING NEW PRODUCT: {row['primary_description']} ({row['product_code']})")
                stmt = sqlalchemy.insert(vendor_toolstream).values(row)
                connection.execute(stmt)
    print(current_time(), 'finished updating catalog')

def main():
    url = f"https://www.toolstream.com/api/v1/GetProducts?&token={AUTH_TOKEN}&format=csv&language=en-GB"

    while True:
        filename = f"toolstream-{time.strftime('%Y%m%d-%H%M%S')}.csv"

        download_csv(url, filename)
        update_database(filename)

        print(current_time(), 'waiting 20 minutes...')
        time.sleep(1200)

if __name__ == "__main__":
    main()