import os
from dotenv import load_dotenv
import requests
import pandas as pd
import time
from sqlalchemy import create_engine
import json
from discord_webhook import DiscordWebhook, DiscordEmbed
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

# Function to send Discord webhook notification
def send_discord_notification(product_code, column, old_value, new_value, discord_webhook_url):
    webhook = DiscordWebhook(url=discord_webhook_url)
    embed = DiscordEmbed(title="Change Detected in Price",
                         description=f"Product_Code: {product_code}\nColumn: {column}\nOld Value: {old_value}\nNew Value: {new_value}",
                         color=242424)
    webhook.add_embed(embed)
    response = webhook.execute()
    print(f"Discord webhook sent with status code: {response.status_code}")

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
        'X-Shopify-Access-Token': f'{API_SECRET}',
        'Content-Type': 'application/json'
    }
    response = requests.request("PUT", url, headers=headers, data=payload)
    print(response.text)

# Function to delete old CSV files
def delete_old_csv_files(directory, max_age_minutes):
    current_time = time.time()
    for filename in os.listdir(directory):
        if filename.endswith(".csv"):
            file_path = os.path.join(directory, filename)
            file_age_minutes = (current_time - os.path.getmtime(file_path)) / 60
            if file_age_minutes > max_age_minutes:
                os.remove(file_path)
                print(f"Deleted old CSV file: {file_path}")

def main():
    url = f"https://www.toolstream.com/api/v1/GetProducts?&token={AUTH_TOKEN}&format=csv&language=en-GB"
    filename = "toolstream.csv"
    discord_webhook_url = "YOUR_DISCORD_WEBHOOK_URL_HERE"

    while True:
        filename = f"toolstream-{time.strftime('%Y%m%d-%H%M%S')}.csv"

        download_csv(url, filename)
        create_database(filename)
        
        # Delete old CSV files older than 24 hours (1440 minutes)
        delete_old_csv_files(".", 1440)

        print(current_time(), 'waiting 20 minutes...')
        time.sleep(1200)

if __name__ == "__main__":
    main()