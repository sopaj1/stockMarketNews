from flask import Flask, render_template, request
import requests
import os

app = Flask(__name__)


API_URL = os.environ.get('API_URL')
# API endpoint URL from the backend

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        # Get the stock symbol from the form
        symbol = request.form.get('symbol')
        if not symbol:
            return render_template('index.html', error='Please enter a symbol')

        # Prepare the JSON payload for the API
        payload = {'symbol': symbol}

        try:
            # Make a POST request to the API
            response = requests.post(API_URL, json=payload)
            response_data = response.json()

            # Handle different status codes from the API
            if response.status_code == 200:
                data = response_data
                return render_template('index.html', data=data)
            elif response.status_code == 404:
                return render_template('index.html', error='No data found for the symbol on today\'s date')
            else:
                error_msg = response_data.get('error', 'Unknown error')
                return render_template('index.html', error=error_msg)

        except Exception as e:
            return render_template('index.html', error=f'Error contacting the API: {str(e)}')

    # For GET requests, render the empty form
    return render_template('index.html')

if __name__ == '__main__':
    app.run(debug=True)