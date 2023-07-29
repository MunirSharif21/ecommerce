import os
import requests
import pandas as pd
import time
from sqlalchemy import create_engine
import json
from discord_webhook import DiscordWebhook, DiscordEmbed

def download_csv(url, filename):
    response = requests.get(url)
    with open(filename, 'wb') as f:
        f.write(response.content)

def create_database(filename):
    df = pd.read_csv(filename)
    db = create_engine("mysql+mysqlconnector://root:pass@localhost/catalog")
    df.to_sql('vendor_toolstream', con=db, if_exists='append', index=False)

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
        'X-Shopify-Access-Token': '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',
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
    url = "https://www.toolstream.com/api/v1/GetProducts?&token=a89500f03df78ec63546d79bc1197834&format=csv&language=en-GB"
    filename = "toolstream.csv"
    discord_webhook_url = "YOUR_DISCORD_WEBHOOK_URL_HERE"

    while True:
        filename = f"toolstream-{time.strftime('%Y%m%d-%H%M%S')}.csv"
        download_csv(url, filename)
        create_database(filename)
        
        # Delete old CSV files older than 24 hours (1440 minutes)
        delete_old_csv_files(".", 1440)

        time.sleep(1200)
if __name__ == "__main__":
    main()