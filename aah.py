import os
import json
import time
import requests
from dotenv import load_dotenv
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import database_exists, create_database
from shopify_api import update_shopify_price, update_shopify_stock

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from seleniumwire import webdriver as wirewebdriver
from seleniumwire import webdriver
from selenium.webdriver.chrome.options import Options

load_dotenv()
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')

AAH_USER = os.getenv('AAH_USER')
AAH_PASS = os.getenv('AAH_PASS')

engine = sqlalchemy.create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASS}@localhost/catalog")

if not database_exists(engine.url):
    create_database(engine.url)

Session = sessionmaker(bind=engine)

def current_time():
    return time.strftime("%Y-%m-%d %H:%M:%S")

def login():
    chrome_options = Options()
    chrome_options.add_argument("profile-directory=PHARMA")
    driver = webdriver.Chrome(options=chrome_options)

    driver.get("https://www.aah.co.uk/s/signin")
    print(current_time(), 'Loading Signin Page')

    # Accept cookies
    accept_button = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler"))
    )
    accept_button.click()
    print(current_time(), 'Accepting Cookies')
    time.sleep(2)

    # Fill in the username and password fields using XPath
    print(current_time(), 'Filling Credentials')
    username_field = driver.find_element(By.XPATH, "//input[@id='input-4']")
    password_field = driver.find_element(By.XPATH, "//input[@id='input-5']")
    username_field.send_keys(AAH_USER)
    password_field.send_keys(AAH_PASS)
    print(current_time(), 'Submitting Credentials')

    # Submit the login form
    submit_button = driver.find_element(By.CSS_SELECTOR, "button[class*='primary-button']")
    submit_button.click()
    time.sleep(10)

    # Navigate to the next URL
    print(current_time(), 'Loading All Product Page')
    driver.get("https://www.enterpriseotc.co.uk/enterprise/AllProducts")
    time.sleep(2)

    # Scroll down and accept cookies
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
    print(current_time(), 'Accepting Cookies')
    click_button = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler"))
    )
    click_button.click()
    time.sleep(5)

    time.sleep(10)
    print(current_time(), 'Finding More Products')
    button = driver.find_element(By.CSS_SELECTOR, '.btn.btn-secondary.center-block.cc_show_more.secondary-button')
    button.click()

    # Specify the wait duration in seconds according to your application's response time
    wait = WebDriverWait(driver, 10)

    # Wait for the network request to complete
    wait.until(EC.presence_of_element_located((By.XPATH, '//*[contains(text(), "Load next 20 products")]')))

    # Retrieve the cookie headers from the request
    return hijack(driver.requests)

def hijack(sel_requests):
    for request in sel_requests:
        if request.method == 'POST' and 'findMore' in request.body.decode('utf-8'):
            print(current_time(), 'Hijacking findMore')
            
            headers = request.headers
            payload = request.body.decode('utf-8')

            payload_dict = json.loads(payload) # open payload as dict
            data_list = payload_dict["data"] # extract data from payload
            data_dict = json.loads(data_list[1]) # access dict within payload data

            index_secondary = data_dict["prodCurrentIndex"]["secondary"] # extract index secondary
            data_dict["prodLimit"] = 1000 # increase product limit

            updated_item = json.dumps(data_dict) # pack the data json back up
            data_list[1] = updated_item

            updated_payload = json.dumps(payload_dict)

            return findMore(headers, updated_payload, index_secondary)
        
def update_product_info(product_info, product_within_db, field_name, update_text, values):
    product_info[field_name] = product_info[field_name].fillna(None)

    if product_info[field_name] != product_within_db[field_name]:

        if field_name == 'price':
            update_shopify_price(product_info['sku'], product_info['price'])
        
        if field_name == 'available':
            update_shopify_stock(product_info['sku'], 100 if product_info['available'] == 1 else 0)

        update_text.append(f"{field_name.upper()} {str(product_within_db[field_name])} -> {product_info[field_name]}")
        values[field_name] = product_info[field_name]

def findMore(headers, updated_payload, index_secondary):
    """
    This function automatically open pages with 1000 visible products
    and inserts them into the catalog.vendor_aah table if they are
    not already there, else the data for that product is updated
    """
    payload_dict = json.loads(updated_payload)
    data_list = payload_dict["data"]
    data_dict = json.loads(data_list[1])

    data_dict["prodCurrentIndex"]["secondary"] = index_secondary # replace previous value
    data_dict["prodLimit"] = 1000 # increase page limit for visible products

    updated_item = json.dumps(data_dict) # pack the data json back up
    data_list[1] = updated_item

    updated_payload = json.dumps(payload_dict)

    # Send the POST request
    response = requests.post(url='https://www.enterpriseotc.co.uk/enterprise/apexremote',
                             headers=headers,
                             data=updated_payload)

    # Check if the response data contains "statusCode": 402
    response_data = json.loads(response.text)

    if response_data[0]["statusCode"] == 402:
        print(current_time(), "Received statusCode 402. Restarting...")
        if not hasattr(findMore, 'restart_attempted'):
            findMore.restart_attempted = True
            return main() # Restart the process by calling the main function
        
    # Reset the restart_attempted flag if it was set before
    if hasattr(findMore, 'restart_attempted'):
        delattr(findMore, 'restart_attempted')
    
    # get a new index_secondary
    index_secondary = response_data[0]['result']['data']['v']['prodCurrentIndex']['v']['secondary']
    product_list = response_data[0]['result']['data']['v']['productList']['v']

    metadata = sqlalchemy.MetaData() # holds table definitions
    vendor = sqlalchemy.Table(
        'vendor_aah', metadata,
        sqlalchemy.Column('name', sqlalchemy.String(255)),
        sqlalchemy.Column('barcode', sqlalchemy.String(255)),
        sqlalchemy.Column('price', sqlalchemy.Float),
        sqlalchemy.Column('trade_price', sqlalchemy.Float),
        sqlalchemy.Column('available', sqlalchemy.Boolean),
        sqlalchemy.Column('min_quantity', sqlalchemy.Integer),
        sqlalchemy.Column('outer_quantity', sqlalchemy.Integer),
        sqlalchemy.Column('sku', sqlalchemy.String(255), unique=True),
        sqlalchemy.Column('last_update', sqlalchemy.DateTime)
    )
    # checkfirst=True ensures the table is only created if it doesn't exist
    metadata.create_all(engine, checkfirst=True)

    # extract info for each product in the page
    for product in product_list:
        product_info = {
            'name': product['v']['sfdcName'],
            'barcode': product['v'].get('EAN1', '') or product['v'].get('EAN2', ''),
            'price': product['v']['MRRP'],
            'available': 1 if product['v']['availabilityMessage'] == 'In Stock' else 0,
            'min_quantity': product['v']['minimumQuantity'],
            'outer_quantity': product['v']['outerQuantity'],
            'sku': product['v']['SKU'],
            'trade_price': product['v']['tradePrice'],
            'last_update': time.strftime("%Y/%m/%d %H:%M:%S")
        }

        with engine.connect() as connection:
            # Make a single query to get all products
            s = sqlalchemy.select(vendor)
            result = connection.execute(s)
            
            # Save products to a dictionary for faster lookup
            db_products = {row['sku']: row for row in result.mappings()}

            product_within_db = db_products.get(product_info['sku'])

            update_text = []
            values = {}

            if product_within_db:
                stmt = sqlalchemy.update(vendor)
                for field_name in product_info.keys():
                    if field_name != 'last_update': # dont check if last_update has been changed because it will always be different
                        update_product_info(product_info, product_within_db, field_name, update_text, values)

                if values:
                    # only update the changed values where the sku matches in the database
                    print(f"\nUPDATED PRODUCT: {product_info['name']} ({product_info['sku']})")

                    # show last update datetime and new update datetime if there were any changes (when values is greater than 0)
                    update_text.append(f"LAST UPDATE: {product_within_db['last_update']} -> {product_info['last_update']}")
                    values['last_update'] = product_info['last_update']

                    for text in update_text:
                        print(text)

                    stmt = stmt.where(vendor.c.sku == product_info['sku']).values(**values)
                    connection.execute(stmt)
                    connection.commit()
            else:
                print(f"\nADDING NEW PRODUCT: {product_info['name']} ({product_info['sku']})")
                stmt = sqlalchemy.insert(vendor).values(product_info)
                connection.execute(stmt)
                connection.commit()

    print("\n" + current_time(), "loading next page...")
    return findMore(headers, updated_payload, index_secondary)

def main():
    login()

if __name__ == "__main__":
    main()