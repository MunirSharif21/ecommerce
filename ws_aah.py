from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import time
import requests
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from seleniumwire import webdriver as wirewebdriver
from seleniumwire import webdriver
from selenium.webdriver.chrome.options import Options
import datetime
import json

def current_time():
    now = datetime.datetime.now()
    timestamp = now.strftime("[%Y-%m-%d %H:%M:%S]")
    return timestamp
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

    # Fill in the username and password fields

    print(current_time(), 'Filling Credentials')
    # Using XPath
    username_field = driver.find_element(By.XPATH, "//input[@id='input-4']")

    # Using XPath
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
    cookie_headers = driver.execute_script('return document.cookie')
    print(current_time(), 'Hijacking findProduct')
    requestss = driver.requests
    for request in requestss:
        if request.method == 'POST' and 'findProduct' in request.body.decode('utf-8'):
            # Retrieve the headers
            headers = request.headers

            # Retrieve the request method
            method = request.method

            # Retrieve the payload
            payload = request.body.decode('utf-8')

            # Print the information
            #print("URL:", 'https://www.enterpriseotc.co.uk/enterprise/apexremote')
            #print("Headers:")
            #for header in headers:
                #print(header + ":", headers[header])
            #print("Method:", method)
            #print("Payload:", payload)
            # Print the curl request
            curl_request = f"curl -X {method} "
            for header in headers:
                curl_request += f"-H '{header}: {headers[header]}' "
            curl_request += f"-d '{payload}' 'https://www.enterpriseotc.co.uk/enterprise/apexremote'"
            findProductHeader = headers
            findProductPayload = payload
            #print("findProduct Request:")
            #print(curl_request)
            continue

    print('---------------------------------')
    print(current_time(), 'Hijacking findMore')
    # Retrieve the request payload from the request
    requestss = driver.requests
    for request in requestss:
        if request.method == 'POST' and 'findMore' in request.body.decode('utf-8'):
            # Retrieve the headers
            headers = request.headers

            # Retrieve the request method
            method = request.method

            # Retrieve the payload
            payload = request.body.decode('utf-8')

            # Print the information
            #print("URL:", 'https://www.enterpriseotc.co.uk/enterprise/apexremote')
            #print("Headers:")
            #for header in headers:
                #print(header + ":", headers[header])
            #print("Method:", method)
            #print("Payload:", payload)
            # Print the curl request
            curl_request = f"curl -X {method} "
            for header in headers:
                curl_request += f"-H '{header}: {headers[header]}' "
            curl_request += f"-d '{payload}' 'https://www.enterpriseotc.co.uk/enterprise/apexremote'"
            findMoreHeader = headers
            findMorePayload = payload

            return findMoreHeader, findMorePayload,findProductHeader, findProductPayload
            #print("findMore Request:")
            #print(curl_request)

def findmore(findMoreHeader, findMorePayload, index_secondary):
    # Replace the 'secondary' value in the payload
    print(current_time(), f"Old Secondary: {index_secondary}")
    payload_dict = json.loads(findMorePayload)
    #print(payload_dict)
    data_list = payload_dict["data"]
    for item in data_list:
        if isinstance(item, str):
            data_dict = json.loads(item)
            data_dict["prodCurrentIndex"]["secondary"] = index_secondary
            data_dict["prodLimit"] = 1000
            updated_item = json.dumps(data_dict)
            data_list[data_list.index(item)] = updated_item

    updated_payload = json.dumps(payload_dict)
    #print(updated_payload)

    # Send the POST request
    response = requests.post(url='https://www.enterpriseotc.co.uk/enterprise/apexremote',
                             headers=findMoreHeader,
                             data=updated_payload)

    # Check if the response data contains "statusCode": 402
    response_data = json.loads(response.text)
    if is_status_code_402(response_data):
        print(current_time(), "Received statusCode 402. Restarting...")
        if not hasattr(findmore, 'restart_attempted'):
            findmore.restart_attempted = True
            main()  # Restart the process by calling the main function
            return  # Return to stop the current execution of findmore

    # Reset the restart_attempted flag if it was set before
    if hasattr(findmore, 'restart_attempted'):
        delattr(findmore, 'restart_attempted')

    #print(response.text)
    index_secondary = response_data[0]["result"]["data"]["v"]["prodCurrentIndex"]["v"]["secondary"]
    print(current_time(), f"New Index: {index_secondary}")
    products = response_data[0]['result']['data']['v']['productList']['v']

    # Print the desired values for each product
    for product in products:
        print("Product Name:", product['v']['sfdcName'])
        print("Availability:", product['v']['availabilityMessage'])
        print("SKU:", product['v']['SKU'])

        # Handle the case when 'EAN1' is not present
        ean1_value = product['v'].get('EAN1', None)
        print("EAN1:", ean1_value if ean1_value is not None else "None")

        print("Minimum Quantity:", product['v']['minimumQuantity'])
        print("Outer Quantity:", product['v']['outerQuantity'])
        print("Price:", product['v']['price'])
        print("Trade Price:", product['v']['tradePrice'])
        print("MRRP:", product['v']['MRRP'])
    return index_secondary

def is_status_code_402(response_data):
    # Check if the response data contains "statusCode": 402
    for data in response_data:
        if "statusCode" in data and data["statusCode"] == 402:
            return True
    return False

def findproduct(findProductHeader,findProductPayload):
    #print(findMoreHeader, findMorePayload)
    print(current_time(), 'Posting FindProduct Request')
    response = requests.post(url = 'https://www.enterpriseotc.co.uk/enterprise/apexremote', headers=findProductHeader, data=findProductPayload)
    response_data = json.loads(response.text)
    # Process the response as needed
    index_secondary = response_data[0]["result"]["data"]["v"]["prodCurrentIndex"]["v"]["secondary"]
    print(current_time(), f"Index Secondary: {index_secondary}")
    return index_secondary
    print(response.text)

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







