import pandas as pd
import requests
from dotenv import load_dotenv
import os
from discord_webhook import DiscordWebhook, DiscordEmbed

load_dotenv()
API_SECRET = os.getenv('SHOPIFY_API_SECRET')

# Global variables
csv_filename = 'rsshop-shopify-catalogue.csv'
df = pd.read_csv(csv_filename)

shop_url = 'https://8edbdd-2.myshopify.com'
headers = {
    'X-Shopify-Access-Token': API_SECRET,
    'Content-Type': 'application/json'
}

def send_discord_webhook(product_identifier, product_id, variant_id, field_name, field_value):
    """
    field_name: string e.g. "price" or "stock"
    field_value: integer
    """
    if field_name == 'price':
        field_value = "£" + field_value
    
    webhook = DiscordWebhook(url='https://discord.com/api/webhooks/1134244504598216785/HASu2dQEm7xIMmRIBoxbxilPUn2pdgV1ya_RLc0nVjUoJZYGroJw5_FRXFElw5j8FZhq')
    embed = DiscordEmbed(title=f'PRODUCT CHANGE: {product_identifier} - {field_name}', color='00ff00')
    embed.set_footer(text='TS Monitor - RSShop', icon_url='https://cdn.discordapp.com/attachments/802914537363865642/1004725175230668820/logo_1.png')
    embed.add_embed_field(name='Product ID', value=f'{product_id}', inline=True)
    embed.add_embed_field(name='Variant ID', value=f'{variant_id}', inline=True)
    embed.add_embed_field(name='SKU', value=f'{product_identifier}', inline=True)
    embed.add_embed_field(name=field_name, value=f'{field_value}', inline=True)

    webhook.add_embed(embed)
    webhook.execute()

def get_product_id(product_identifier):
    product_row = df[df['SKU'] == product_identifier] # Find row with matching product_identifier

    if not product_row.empty: # check if product exists
        product_id = int(product_row.iloc[0]['Product ID'])
        variant_id = int(product_row.iloc[0]['Variant ID'])
        inventory_id = int(product_row.iloc[0]['Inventory Item ID'])
        return product_id, variant_id, inventory_id

def update_shopify_price(product_identifier, new_price):
    product_id, variant_id, _ = get_product_id(product_identifier)

    # Endpoint to update the product price in Shopify
    endpoint = f"{shop_url}/admin/api/2023-07/products/{product_id}.json"

    # Construct the data to update the price in the Shopify request
    data = {
        "product": {
            "id": product_id,
            "variants": [
                {
                    "id": variant_id,
                    "price": str(new_price)
                }
            ]
        }
    }

    # Make the POST request to Shopify's API
    response = requests.put(endpoint, json=data, headers=headers)

    # Check the response status code to determine if the update was successful
    if response.status_code == 200:
        print(f"Price updated successfully for product ID {product_id} with variant {variant_id} SKU {product_identifier}")
        # send_discord_webhook_price(product_identifier, new_price, product_id, variant_id)
        send_discord_webhook(product_identifier, product_id, variant_id, 'price', new_price)
    else:
        print(f"Failed to update price for product ID {product_id} with variant {variant_id} SKU {product_identifier}. Status code: {response.status_code}, Response: {response.text}")

def update_shopify_stock(product_identifier, new_quantity):
    product_id, variant_id, inventory_id = get_product_id(product_identifier)

    # Endpoint to update the product inventory quantity in Shopify
    endpoint = f"{shop_url}/admin/api/2023-07/inventory_levels/set.json"

    # Construct the data to update the stock in the Shopify request
    data = {
        "location_id": 88542675242,
        "inventory_item_id": inventory_id,
        "available": new_quantity
    }

    # Make the POST request to Shopify's API
    response = requests.post(endpoint, json=data, headers=headers)
    
    # Check the response status code to determine if the update was successful
    if response.status_code == 200:
        print(f"Stock updated successfully for inv ID {inventory_id}")
        send_discord_webhook(product_identifier, product_id, inventory_id, 'stock', new_quantity)

    else:
        print(f"Failed to update stock for inv ID {inventory_id}. Status code: {response.status_code}, Response: {response.text}")