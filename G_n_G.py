#import the required libraries
import io
import re
import pandas as pd
import PyPDF2
from fastapi import FastAPI, File, UploadFile
from fastapi.responses import JSONResponse
import spacy
nlp = spacy.load("model")
app = FastAPI()
# Load product data from CSV
product = pd.read_csv('/content/Products_rows.csv')

def extract_text_from_pdf(file_path):
        """Extract text from each page of the specified PDF.

    Args:
        file_path (str): Path to the PDF file.

    Returns:
        str: Concatenated text from all pages of the PDF.
    """
    with open(file_path, "rb") as file:
        pdf_reader = PyPDF2.PdfReader(file)
        all_text = ""
        for page in pdf_reader.pages:
            all_text += page.extract_text()
    return all_text

def get_campaign_by_id(df, campaign_id):
      """Retrieve campaign name by its ID from the product DataFrame.

    Args:
        df (pandas.DataFrame): Product DataFrame.
        campaign_id (int): ID of the campaign to retrieve.

    Returns:
        str or None: Name of the campaign if found, None otherwise.
    """
    result = df.loc[df['campaign_id'] == campaign_id, 'campaign']
    if not result.empty:
        return result.values[0]
    else:
        return None

def parse_text(text):
      """Parse text to extract relevant data using regular expressions.

    Args:
        text (str): Text extracted from PDF.

    Returns:
        dict: Parsed data in dictionary format.
    """

    data = {}
    try:
        data['offertenummer'] = re.search(r'Offertenummer:\s*(\d+)', text).group(1)
    except:
        data['offertenummer'] = None
    try:
        data['created_at'] = re.search(r'Gemaakt op:\s+(\d{2}/\d{2}/\d{4})', text).group(1)
    except:
        data['created_at'] = None
    try:
        data['name'] = re.search(r'Naam bedrijf\s+(.+)', text).group(1)
    except:
        data['name'] = None
    try:
        data['street'] = re.search(r'(\d{4} .+)\n(.+)', text).group(2)
    except:
        data['street'] = None
    try:
        data['address'] = re.search(r'(\d{4} .+)\n(.+)', text).group(1)
    except:
        data['address'] = None
    try:
        data['place'] = re.search(r'(\d{4})', text).group(0)
    except:
        data['place'] = None
    try:
        data['postal'] = re.search(r'\d{4}', text).group(0)
    except:
        data['postal'] = None
    try:
        data['vatnr'] = re.search(r'BTW\s+(\w+)', text).group(1)
    except:
        data['vatnr'] = None
    try:
        data['price_without_vat'] = float(re.search(r'Totaal zonder BTW:\s*€\s*([\d,.]+)', text).group(1).replace('.', '').replace(',', '.'))
    except:
        data['price_without_vat'] = None
    try:
        data['total_price'] = float(re.search(r'Eindtotaal:\s*€\s*([\d,.]+)', text).group(1).replace('.', '').replace(',', '.'))
    except:
        data['total_price'] = None
    try:
        data['vat'] = int(re.search(r'BTW\s*\((\d+)%\)', text).group(1))
    except:
        data['vat'] = None
    try:
        data['items'] = re.findall(r'(\d+,\d+)\n\s+€ (\d+,\d+)', text)
    except:
        data['items'] = None

    # Extract product details dynamically
    services = re.findall(r'(?P<service>[\w\s]+)\s+(?P<region>[\w\s]+)\s+(?P<asset_type>[\w\s]+)\s+(?P<asset_amount>\d+)\s+(?P<asset_value>\d+,\d+)\s+€ (?P<product_price>\d+,\d+)', text)
    data['services'] = []
    for idx, service in enumerate(services, start=1):
        data['services'].append({
            "service_id": idx,
            "service": service[0],
            "region": service[1],
            "asset_type": service[2],
            "asset_amount": int(service[3]),
            "asset_value": float(service[4].replace(',', '.')),
            "product_price": float(service[5].replace(',', '.'))
        })
    return data

def preprocess_data(data):
     """Preprocess extracted data for further processing.

    Args:
        data (dict): Extracted data.

    Returns:
        pandas.DataFrame: Processed DataFrame.
    """
    try:
        items = [{"price": float(item[0].replace(',', '.')), "product_price": float(item[1].replace(',', '.'))} for item in data['items']]
    except:
        items = []
    df = pd.DataFrame(items)
    return df

def convert_to_json(data, df):
      """Convert parsed data and DataFrame to JSON format.

    Args:
        data (dict): Parsed data.
        df (pandas.DataFrame): Processed DataFrame.

    Returns:
        dict: Converted data in JSON format.
    """
    json_data = {
        # JSON structure with converted data
        "Campaigns": {
            "id": data.get("offertenummer"),
            "created_at": data.get("created_at"),
            "price": data.get("price_without_vat"),
            "pricetotal": data.get("total_price"),
            "VAT": data.get("vat"),
            "campname": get_campaign_by_id(product, data.get("offertenummer")),
        },
        "Clients": {
            "name": data.get("name"),
            "street": data.get("street"),
            "place": data.get("place"),
            "number": "N/A",  # Adjust according to specific PDF structure
            "postal": data.get("postal"),
            "vatnr": data.get("vatnr")
        },
        "Contacts": {
            "name": data.get("name")
        },
        "Logs": {
            "note": "Campaign Created from Import",
            "campaign_id": data.get("offertenummer")
        },
        "Products": [
            {
                "created_at": data.get("created_at"),
                "campaign": get_campaign_by_id(product, data.get("offertenummer")),
                "start": ["19/09/2024"],
                "end": ["19/10/2024"],
                "service": service['service'],
                "region": service['region'],
                "asset_type": service['asset_type'],
                "asset_amount": service['asset_amount'],
                "asset_value": service['asset_value'],
                "discount": "FALSE",
                "campaign_id": data.get("offertenummer"),
                "product_price": service['product_price'],
                "service_id": service['service_id']
            } for service in data['services']
        ]
    }
    return json_data

@app.post("/upload")
async def upload(file: UploadFile = File(...)):
    """Endpoint to handle file uploads and process PDF data.

    Args:
        file (UploadFile): Uploaded PDF file.

    Returns:
        JSONResponse: JSON response with processed data.
    """
    content = await file.read()
    file_stream = io.BytesIO(content)
    text = extract_text_from_pdf(file_stream)
    data = parse_text(text)
    df = preprocess_data(data)
    json_data = convert_to_json(data, df)
    return JSONResponse(content=json_data)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)