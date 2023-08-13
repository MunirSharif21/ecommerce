import os
import time
import requests
from dotenv import load_dotenv
import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import database_exists, create_database
from shopify_api import update_shopify_price, update_shopify_stock

load_dotenv()
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')

API_SECRET = os.getenv('SHOPIFY_API_SECRET')

engine = sqlalchemy.create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASS}@localhost/catalog")

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
    df = pd.read_csv(filename).iloc[:, :-1] # remove last column
    df = df.replace(float('nan'), None)
    df.drop('Catalogue Identifier', axis=1, inplace=True)
    df.columns = df.columns.str.lower().str.replace(' ', '_') # fix column names

    metadata = sqlalchemy.MetaData()
    vendor = sqlalchemy.Table(
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
        count = session.query(sqlalchemy.func.count(vendor.c.bar_code)).scalar()
        if not count:
            print(current_time(), "table is empty")
            df.to_sql(vendor.fullname, con=engine, if_exists='append', index=False, chunksize=500)
            return print(current_time(), "entire csv appended")
    return update_catalog(vendor, df)

def compare_product_data(row_product, db_product):
    update_text, values = [], {}

    for field, old_value in db_product.items():
        if row_product[field] != old_value:
            update_text.append(f"{field.upper()} {old_value} -> {row_product[field]}")
            values[field] = row_product[field]

            if field == 'price':
                update_shopify_price(row_product['bar_code'], row_product['price'])

            if field == 'stock_no':
                update_shopify_stock(row_product['bar_code'], row_product['stock_no'])
        
    return values, update_text

def update_catalog(vendor, df):
    print(current_time(), 'start updating catalog...')
    with engine.connect() as connection:
        # Make a single query to get all products
        s = sqlalchemy.select(vendor)
        result = connection.execute(s)

        # Save products to a dictionary for faster lookup
        db_products = {row['bar_code']: row for row in result.mappings()}

        # Prepare list to hold insert statements
        insert_stmts = []

        for row_product in df.itertuples(index=False):
            barcode = row_product.bar_code
            
            # Use dict for faster lookup, no db query is made in this loop
            db_product = db_products.get(barcode)

            if db_product:
                values, update_text = compare_product_data(row_product._asdict(), db_product)

                if values:
                    stmt = sqlalchemy.update(vendor)
                    print(f"\nUPDATING PRODUCT: {row_product.item_description} ({barcode})")

                    for text in update_text:
                        print(text)

                    stmt = stmt.where(vendor.c.bar_code == barcode).values(**values)
                    connection.execute(stmt)
                    connection.commit()
            else:
                stmt = sqlalchemy.update(vendor)
                print(f"\nADDING NEW PRODUCT: {row_product.item_description} ({barcode})")
                stmt = sqlalchemy.insert(vendor).values(row_product._asdict())
                insert_stmts.append(stmt)

        if insert_stmts:
            # Concatenate all the insert statements into a single SQL expression
            connection.execute(sqlalchemy.union_all(*insert_stmts))
            connection.commit()

    print(current_time(), 'finished updating catalog')

def main():
    url = "https://b2b.drapertools.com/products/pricefiles/draper_list_prices_uk.csv"

    while True:
        filename = f"draper-{time.strftime('%Y%m%d-%H%M%S')}.csv"

        download_csv(url, filename)
        update_database(filename)
        
        print(current_time(), 'waiting 20 minutes...')
        time.sleep(1200)

if __name__ == "__main__":
    main()