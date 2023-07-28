import os
import requests
import pandas as pd
import time
import json
from discord_webhook import DiscordWebhook, DiscordEmbed

# Function to download the CSV file
def download_csv(url, filename):
    response = requests.get(url)
    with open(filename, 'wb') as f:
        f.write(response.content)

# Function to compare two CSV files
def compare_csv_files(old_file, new_file, columns_to_monitor, discord_webhook_url):
    df_old = pd.read_csv(old_file, usecols=columns_to_monitor + ["Product_Code"])
    df_new = pd.read_csv(new_file, usecols=columns_to_monitor + ["Product_Code"])

    # Find rows where values have changed
    changed_rows = df_old[df_old[columns_to_monitor].ne(df_new[columns_to_monitor]).any(axis=1)]

    if not changed_rows.empty:
        for index, row in changed_rows.iterrows():
            product_code = row["Product_Code"]
            for col in columns_to_monitor:
                old_value = row[col]
                new_value = df_new.at[index, col]
                if old_value != new_value:
                    print(f"Change detected for Product_Code {product_code}, Column: {col}")
                    print(f"Old Value: {old_value}, New Value: {new_value}")
                    if col in ["Net_Price", "Promotional_Price"]:
                        # Send Discord webhook notification
                        send_discord_notification(product_code, col, old_value, new_value, discord_webhook_url)
                    elif col == "Stock":
                        # Update Shopify product using the Shopify API
                        update_shopify_product(product_code, new_value)

    return not df_old.equals(df_new)

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

# Initial download
url = "https://www.toolstream.com/api/v1/GetProducts?&token=a89500f03df78ec63546d79bc1197834&format=csv&language=en-GB"
filename = "toolstream.csv"
columns_to_monitor = ["Stock", "Net_Price", "Promotional_Price"]
discord_webhook_url = "YOUR_DISCORD_WEBHOOK_URL_HERE"

download_csv(url, filename)

# Monitor changes every 20 minutes
while True:
    time.sleep(1200)  # 20 minutes in seconds
    new_filename = f"toolstream-{time.strftime('%Y%m%d-%H%M%S')}.csv"
    download_csv(url, new_filename)

    if compare_csv_files(filename, new_filename, columns_to_monitor, discord_webhook_url):
        print("Changes detected in the monitored columns!")
        # Do something with the changes (e.g., notify, process data, etc.)

    # Update the filename to the latest version
    filename = new_filename

    # Delete old CSV files older than 24 hours (1440 minutes)
    csv_directory = "."
    max_age_minutes = 1440
    delete_old_csv_files(csv_directory, max_age_minutes)
