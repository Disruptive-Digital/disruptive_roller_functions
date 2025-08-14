import os
import json
import requests
from google.cloud import bigquery

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

    return response.json().get("access_token")


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
    return response


# if __name__ == '__main__':
#     print(run_pipeline())