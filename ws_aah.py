from datetime import datetime
import json
import time
import requests
import mysql.connector
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from seleniumwire import webdriver as wirewebdriver
from seleniumwire import webdriver
from selenium.webdriver.chrome.options import Options

def current_time():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

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
    username_field.send_keys("Rachel1966")
    password_field.send_keys("W1ndmillh2@")
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
            main()  # Restart the process by calling the main function
            return  # Return to stop the current execution of findMore

    # Reset the restart_attempted flag if it was set before
    if hasattr(findMore, 'restart_attempted'):
        delattr(findMore, 'restart_attempted')
    
    # get a new index_secondary
    index_secondary = response_data[0]['result']['data']['v']['prodCurrentIndex']['v']['secondary']
    product_list = response_data[0]['result']['data']['v']['productList']['v']

    db = mysql.connector.connect(
        user='root',
        password='pass'
    )
    cursor = db.cursor()
    cursor.execute('CREATE DATABASE IF NOT EXISTS catalog')
    cursor.execute('USE catalog')

    cursor.execute("""
                   CREATE TABLE IF NOT EXISTS vendor_aah (
                   name VARCHAR(255),
                   barcode VARCHAR(255) UNIQUE,
                   price FLOAT,
                   trade_price FLOAT,
                   mrrp FLOAT,
                   available BOOLEAN,
                   min_quantity INT,
                   outer_quantity INT,
                   sku VARCHAR(255),
                   last_update DATETIME
                   )
                   """)

    # Print the desired values for each product
    for product in product_list:
        barcode = product['v'].get('EAN1') or product['v'].get('EAN2')
        if not barcode:
            continue

        name = product['v']['sfdcName']
        price = product['v']['MRRP']

        available = product['v']['availabilityMessage']
        if available == 'In stock':
            available = 1
        else:
            available = 0
        
        min_quantity = product['v']['minimumQuantity']
        outer_quantity = product['v']['outerQuantity']
        sku = product['v']['SKU']

        trade_price = product['v']['tradePrice']
        mrrp = product['v']['MRRP']

        last_update = datetime.now().strftime("%Y/%m/%d %H:%M:%S")

        cursor.execute("""
                    SELECT *
                    FROM vendor_aah
                    WHERE barcode = %s
                    """, (barcode, ))
        
        exist = cursor.fetchone()

        if exist:
            cursor.execute("""
                        UPDATE vendor_aah
                           SET price = %s,
                           trade_price = %s,
                           mrrp = %s,
                           available = %s,
                           min_quantity = %s,
                           outer_quantity = %s,
                           last_update = %s
                        WHERE barcode = %s
                        """, (price, trade_price, mrrp, available, min_quantity, outer_quantity, last_update, barcode))
        else:
            cursor.execute("""
                        INSERT INTO vendor_aah (
                           name,
                           barcode,
                           price,
                           trade_price,
                           mrrp,
                           available,
                           min_quantity,
                           outer_quantity,
                           sku,
                           last_update
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (name, barcode, price, trade_price, mrrp, available, min_quantity, outer_quantity, sku, last_update))

    db.commit() # save changes
    db.close() # close connection
    print(current_time(), "\nloading next page...")
    return findMore(headers, updated_payload, index_secondary)

def main():
    login()

if __name__ == "__main__":
    main()