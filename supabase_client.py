import asyncio
import os
import json
from supabase import create_client
from uuid import uuid4
from datetime import datetime 
from dotenv import load_dotenv
import os



load_dotenv() 

def main():
    SUPABASE_URL = os.getenv('SUPABASE_URL')
    SUPABASE_KEY = os.getenv('SUPABASE_KEY')

    directory = 'expected_results'
    asyncio.run(process_files(directory, supabase))

async def process_files(directory, supabase):
    for filename in os.listdir(directory):
        if filename.endswith('.json'):
            filepath = os.path.join(directory, filename)
            await process_file(filepath, supabase)

async def process_file(filepath, supabase):
    try:
        with open(filepath, 'r') as file:
            json_data = json.load(file)
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON from file {filepath}: {e}")
        return
    except IOError as e:
        print(f"Error reading file {filepath}: {e}")
        return

    if not isinstance(json_data, dict):
        print(f"Invalid JSON format in file {filepath}. Expected a dictionary.")
        return

    enforce_data_types_and_defaults(json_data)

    try:
        await upsert_data('Clients', json_data['Clients'], supabase)
        await upsert_data('Campaigns', json_data['Campaigns'], supabase)
        await upsert_data('Contacts', json_data['Contacts'], supabase)
        for log in json_data.get('Logs', []):
            if isinstance(log, dict):
                await upsert_data('Logs', log, supabase)
            else:
                print(f"Skipping invalid log entry: {log}")
        for product in json_data.get('Products', []):
            await upsert_data('Products', product, supabase)
    except Exception as e:
        print(f"Error processing data from file {filepath}: {e}")


def enforce_data_types_and_defaults(json_data):
    # Ensure campaigns dictionary and set defaults
    campaigns = json_data.get('Campaigns', {})

    # Enforce data types and defaults for the Campaigns table
    campaigns = json_data['Campaigns']
    campaigns['campname'] = str(campaigns.get('campname', ''))
    campaigns['campstatus'] = str(campaigns.get('campstatus', 'Todo'))
    campaigns['contact'] = int(campaigns.get('contact', 0))
    campaigns['created_at'] = str(campaigns.get('created_at', datetime.now().isoformat()))
    campaigns['created_by'] = int(campaigns.get('created_by', 1))
    campaigns['delay'] = int(campaigns.get('delay', 0)) if campaigns.get('delay') is not None else None
    campaigns['edited_by'] = int(campaigns.get('edited_by', 1))
    campaigns['end'] = str(campaigns.get('end', ''))
    campaigns['id'] = int(campaigns['id'])
    campaigns['invoicenr'] = int(campaigns.get('invoicenr', 0)) if campaigns.get('invoicenr') is not None else None
    campaigns['last_edit'] = str(campaigns.get('last_edit', ''))
    campaigns['last_update'] = str(campaigns.get('last_update', ''))
    campaigns['logs'] = campaigns.get('logs', [])
    campaigns['price'] = float(campaigns.get('price', 0.0))
    campaigns['pricetotal'] = float(campaigns.get('pricetotal', 0.0))
    campaigns['products'] = campaigns.get('products', [])  # JSON
    campaigns['start'] = str(campaigns.get('start', ''))
    campaigns['uuid'] = str(campaigns.get('uuid', str(uuid4())))
    campaigns['VAT'] = int(campaigns.get('VAT', 0))

    # Enforce data types and defaults for the Clients table
    clients = json_data['Clients']
    clients['created_at'] = str(clients.get('created_at', datetime.now().isoformat()))
    clients['gen_email'] = str(clients.get('gen_email', ''))
    clients['id'] = int(clients.get('id'))
    clients['name'] = str(clients.get('name', ''))
    clients['number'] = str(clients.get('number', ''))
    clients['place'] = str(clients.get('place', ''))
    clients['postal'] = int(clients.get('postal', 0))
    clients['street'] = str(clients.get('street', ''))
    clients['vatnr'] = str(clients.get('vatnr', ''))

    # Enforce data types for the Contacts table
    contacts = json_data['Contacts']
    contacts['client_id'] = int(contacts['client_id'])
    contacts['created_at'] = str(contacts.get('created_at', datetime.now().isoformat()))
    contacts['email'] = str(contacts.get('email', ''))
    contacts['id'] = int(contacts['id'])
    contacts['name'] = str(contacts.get('name', ''))

    # Enforce data types for the Logs table
    for log in json_data['Logs']:
        log['campaign'] = int(log['campaign'])
        log['created_at'] = str(log.get('created_at', datetime.now().isoformat()))
        log['file'] = bool(log.get('file', False))
        log['id'] = int(log['id'])
        log['logtype'] = str(log.get('logtype', 'SYSTEM'))
        log['note'] = str(log.get('note', ''))
        log['user_id'] = str(log.get('user_id', ''))

    # Enforce data types for the Products table
    for product in json_data['Products']:
        product['asset_amount'] = int(product.get('asset_amount', 0))
        product['asset_type'] = str(product.get('asset_type', ''))
        product['asset_value'] = float(product.get('asset_value', 0.0))
        product['campaign'] = str(product.get('campaign', ''))
        product['campaign_id'] = int(product.get('campaign_id', 0))
        product['created_at'] = str(product.get('created_at', datetime.now().isoformat()))
        product['discount'] = bool(product.get('discount', False))
        product['discount_percent'] = float(product.get('discount_percent', 0.0))
        product['discount_type'] = str(product.get('discount_type', ''))
        product['discount_value'] = float(product.get('discount_value', 0.0))
        product['end'] = product.get('end', [])  # JSON
        product['product_id'] = int(product['product_id'])
        product['product_price'] = float(product.get('product_price', 0.0))
        product['product_status'] = str(product.get('product_status', ''))
        product['region'] = str(product.get('region', ''))
        product['service'] = str(product.get('service', ''))
        product['service_id'] = int(product.get('service_id', 0))
        product['service_type'] = str(product.get('service_type', ''))
        product['start'] = product.get('start', [])  # JSON

async def upsert_data(table_name, data, supabase):
    if not isinstance(data, dict):
        print(f"Skipping upsert for {table_name} due to invalid data type.")
        return
    response = await supabase.table(table_name).upsert(data).execute()
    if response.error:
        print(f"Error upserting data into {table_name}: {response.error}")
    else:
        print(f"Data upserted into {table_name} successfully.")

if __name__ == "__main__":
    main()
