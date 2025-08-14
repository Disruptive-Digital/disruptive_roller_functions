import os
import json
import logging
import requests
from datetime import datetime
from google.cloud import bigquery
from flask import Flask, request, jsonify

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)


def get_roller_auth_token() -> str:

    url = "https://api.roller.app/token"

    payload = json.dumps({
        "client_id": os.getenv("clientid"),
        "client_secret": os.getenv("secretid")
    })
    headers = {
        'Content-Type': 'application/json'  
    }

    response = requests.post(url, headers=headers, data=payload)

    return response.json.access_token


def get_roller_revenue(startDate: str, 
                       endDate: str, 
                       pageNumber: int = 0,
                    ) -> str:

    if pageNumber > 0:
        url = f"https://api.roller.app/reporting/revenue-entries?pageNumber={pageNumber}&endDate={endDate}&startDate={startDate}"
    else:
        url = f"https://api.roller.app/reporting/revenue-entries?endDate={endDate}&startDate={startDate}"

    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {get_roller_auth_token()}'
    }

    response = requests.get(url, headers=headers)
    return response.json

def run_pipeline()
    json_data = get_roller_revenue(startDate="2024-08-01", endDate="2024-08-02")
    return json_data

# Cloud Run endpoints
@app.route('/', methods=['GET', 'POST'])
def main():
    """Main endpoint for Cloud Run"""
    result = run_pipeline()
    
    if result['status'] == 'success':
        return jsonify(result), 200
    else:
        return jsonify(result), 500

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({'status': 'healthy'}), 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)