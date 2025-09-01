from flask import Flask, request
from functions import get_roller_revenue, merge_to_bigquery
import pandas as pd
from datetime import date, timedelta

app = Flask(__name__)

def run_pipeline():
    
    # In local, run the url like this: 
    # http://0.0.0.0:8080
    today = date.today() - timedelta(days=1)
    yesterday = date.today() - timedelta(days=2)
    endDate = today.strftime("%Y-%m-%d")
    startDate = yesterday.strftime("%Y-%m-%d")

    location = request.headers.get('location')

    print(f"Running for location {arlington}: {startDate} to {endDate}")

    df_list = []
    current_page = 1
    total_pages = current_page + 1

    while current_page <= total_pages:
        print(current_page)
        data_response = get_roller_revenue(startDate=startDate, 
                                           endDate=endDate, 
                                           pageNumber=current_page,
                                           location=location)
        json_data = data_response.json()

        df = pd.DataFrame(json_data)
        print(df.shape)
        df_list.append(df)

        total_pages = json_data.get("totalPages")
        current_page = json_data.get("currentPage") + 1

    # Combine all into one DataFrame
    final_df = pd.concat(df_list, ignore_index=True)
    print(f"Loading records: {final_df.shape}")

    project_name = "earnest-dogfish-465412-p1"
    table_name = "revenue"
    merge_to_bigquery(df=final_df,
                      project_name=project_name,
                      dataset_name=f"slickcity_{location}_roller",
                      table_name=table_name)
    print(f"Successfully merged data from dates {startDate} to {endDate}")

    return data_response


# Cloud Run endpoints
@app.route('/', methods=['GET', 'POST'])
def main():
    """Main endpoint for Cloud Run"""

    result = run_pipeline()
    
    if result.status_code == 'success':
        return result.json(), 200
    else:
        return result.json(), 500

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({'status': 'healthy'}), 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)