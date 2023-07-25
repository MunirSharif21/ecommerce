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
    # Create ChromeOptions object
    chrome_options = Options()

    # Set headless mode to True
    chrome_options.headless = False

    # Create WebDriver instance with headless mode enabled
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

    time.sleep(20)
    print(current_time(), 'Loading All Product Page')

    # Navigate to the next URL
    driver.get("https://www.enterpriseotc.co.uk/enterprise/AllProducts")

    time.sleep(2)

    # Scroll down
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
    print(current_time(), 'Accepting Cookies')

    # Click the button
    click_button = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler"))
    )
    click_button.click()

    time.sleep(10)
    print(current_time(), 'Finding More Products')
    button = driver.find_element(By.CSS_SELECTOR, '.btn.btn-secondary.center-block.cc_show_more.secondary-button')
    button.click()

    # Specify the wait duration in seconds according to your application's response time
    wait = WebDriverWait(driver, 10)

    # Wait for the network request to complete
    wait.until(EC.presence_of_element_located((By.XPATH, '//*[contains(text(), "Load next 20 products")]')))

    # Retrieve the cookie headers from the request
    requestss = driver.requests

    for request in requestss:
        if request.method == 'POST':
            if 'findProduct' in request.body.decode('utf-8'):
                print(current_time(), 'Hijacking findProduct')
                headers = request.headers
                method = request.method
                payload = request.body.decode('utf-8')

                curl_request = f"curl -X {method} "
                for header in headers:
                    curl_request += f"-H '{header}: {headers[header]}' "
                curl_request += f"-d '{payload}' 'https://www.enterpriseotc.co.uk/enterprise/apexremote'"
                findProductHeader = headers
                findProductPayload = payload

            if 'findMore' in request.body.decode('utf-8'):
                print('---------------------------------')
                print(current_time(), 'Hijacking findMore')

                headers = request.headers
                method = request.method
                payload = request.body.decode('utf-8')

                curl_request = f"curl -X {method} "

                for header in headers:
                    curl_request += f"-H '{header}: {headers[header]}' "
                curl_request += f"-d '{payload}' 'https://www.enterpriseotc.co.uk/enterprise/apexremote'"
                findMoreHeader = headers
                findMorePayload = payload
    return findMoreHeader, findMorePayload, findProductHeader, findProductPayload
    

def findmore(findMoreHeader, findMorePayload, index_secondary):
    # Replace the 'secondary' value in the payload
    print(current_time(), f"Old Secondary: {index_secondary}")
    payload_dict = json.loads(findMorePayload)
    data_list = payload_dict["data"]

    for item in data_list:
        if isinstance(item, str):
            data_dict = json.loads(item)
            data_dict["prodCurrentIndex"]["secondary"] = index_secondary
            data_dict["prodLimit"] = 1000
            updated_item = json.dumps(data_dict)
            data_list[data_list.index(item)] = updated_item

    updated_payload = json.dumps(payload_dict)

    # Send the POST request
    response = requests.post(url='https://www.enterpriseotc.co.uk/enterprise/apexremote',
                             headers=findMoreHeader,
                             data=updated_payload)

    # Check if the response data contains "statusCode": 402
    response_data = json.loads(response.text)

    if response_data[0]["statusCode"] == 402:
        print(current_time(), "Received statusCode 402. Restarting...")
        if not hasattr(findmore, 'restart_attempted'):
            findmore.restart_attempted = True
            main()  # Restart the process by calling the main function
            return  # Return to stop the current execution of findmore

    # Reset the restart_attempted flag if it was set before
    if hasattr(findmore, 'restart_attempted'):
        delattr(findmore, 'restart_attempted')
    
    index_secondary = response_data[0]["result"]["data"]["v"]["prodCurrentIndex"]["v"]["secondary"]
    print(current_time(), f"New Index: {index_secondary}")
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
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """, (name, barcode, price, trade_price, mrrp, available, min_quantity, outer_quantity, sku, last_update))

    db.commit() # save changes
    db.close() # close connection
    return index_secondary

def findproduct(findProductHeader,findProductPayload):
    print(current_time(), 'Posting FindProduct Request')
    response = requests.post(url = 'https://www.enterpriseotc.co.uk/enterprise/apexremote', headers=findProductHeader, data=findProductPayload)
    response_data = json.loads(response.text)
    # Process the response as needed
    index_secondary = response_data[0]["result"]["data"]["v"]["prodCurrentIndex"]["v"]["secondary"]
    print(current_time(), f"Index Secondary: {index_secondary}")
    return index_secondary

def main():
    # Call the login function to retrieve the headers and payload
    findMoreHeader, findMorePayload, findProductHeader, findProductPayload = login()
    print('---------------------------------')

    index_secondary = findproduct(findProductHeader, findProductPayload)
    print('---------------------------------')

    while True:
        index_secondary = findmore(findMoreHeader, findMorePayload, index_secondary)

        # Wait for a while before making the next request
        time.sleep(10)

if __name__ == "__main__":
    main()