import os
from dotenv import load_dotenv
import requests
import pandas as pd
import time
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import database_exists, create_database
from shopify_api import update_shopify_price, update_shopify_stock

load_dotenv()
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')

AUTH_TOKEN = os.getenv('TOOLSTREAM_AUTH_TOKEN')

engine = sqlalchemy.create_engine(f"mysql+mysqlconnector://{DB_USER}:{DB_PASS}@localhost/catalog")

if not database_exists(engine.url):
    create_database(engine.url)

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
    df = df.replace(float('nan'), None)
    df.columns = df.columns.str.lower()
    
    metadata = sqlalchemy.MetaData()
    vendor = sqlalchemy.Table(
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
        count = session.query(sqlalchemy.func.count(vendor.c.product_code)).scalar()
        if not count:
            print(current_time(), "table is empty")
            df.to_sql(vendor.fullname, con=engine, if_exists='append', index=False, chunksize=500)
            return print(current_time(), "entire csv appended")
    return update_catalog(vendor, df)

def compare_product_data(row_product, db_product):
    values, update_text = {}, []

    for field, old_value in db_product.items():
        if row_product[field] != old_value:
            update_text.append(f"{field.upper()} {old_value} -> {row_product[field]}")
            values[field] = row_product[field]

            if field == 'net_price':
                update_shopify_price(row_product['product_code'], row_product['net_price'])

            if field == 'stock':
                update_shopify_stock(row_product['product_code'], row_product['stock'])

    return values, update_text

def update_catalog(vendor, df):
    print(current_time(), 'start updating catalog...')
    with engine.connect() as connection:
        # Make a single query to get all products
        s = sqlalchemy.select(vendor)
        result = connection.execute(s)

        # Save products to a dictionary for faster lookup
        db_products = {row['product_code']: row for row in result.mappings()}

        # Prepare list to hold insert statements
        insert_stmts = []

        for row_product in df.itertuples(index=False):
            product_code = row_product.product_code
            
            # Use dict for faster lookup, no db query is made in this loop
            db_product = db_products.get(product_code)

            if db_product:
                values, update_text = compare_product_data(row_product._asdict(), db_product)
                
                if values:
                    stmt = sqlalchemy.update(vendor)
                    print(f"\nUPDATING PRODUCT: {row_product.primary_description} ({product_code})")
                    
                    for text in update_text:
                        print(text)

                    stmt = stmt.where(vendor.c.product_code == product_code).values(**values)
                    connection.execute(stmt)
                    connection.commit()
            else:
                stmt = sqlalchemy.update(vendor)
                print(f"\nADDING NEW PRODUCT: {row_product.primary_description} ({product_code})")
                stmt = sqlalchemy.insert(vendor).values(row_product._asdict())
                insert_stmts.append(stmt)
                
        if insert_stmts:
            # Concatenate all the insert statements into a single SQL expression
            insert_stmt = sqlalchemy.union_all(*insert_stmts)
            connection.execute(insert_stmt)
            connection.commit()

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