import logging
from flask import Flask, request
from functions import get_roller_revenue

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

def run_pipeline():

    startDate = request.args.get('startDate')
    endDate = request.args.get('endDate')

    data_response = get_roller_revenue(startDate=startDate, endDate=endDate)

    # json_data = data_response.json()

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