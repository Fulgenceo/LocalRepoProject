import csv
import json
import requests
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# Configuration
AUTH_URL = "https://api.dha.go.ke/v1/hie-auth?key=finsprint"
FETCH_URL = "https://api.dha.go.ke/v3/client-registry/fetch-client"
AUTH_HEADER = {
    'Authorization': 'Basic Zmluc3ByaW50OjdTTVpobGpWSXhnUXZK',
    'Cookie': 'full_name=Guest; sid=Guest; system_user=no; user_id=Guest; user_image='
}

THREAD_COUNT = 20  # Number of parallel threads
CALLS_PER_SECOND = 40  # Limit per thread for a total of 300 calls/sec
CALL_DELAY = 0.5 / CALLS_PER_SECOND  # Delay between calls per thread

# Global counter and lock
counter = 0
counter_lock = Lock()


def authenticate():
    """Authenticate and retrieve a plain text token."""
    response = requests.get(AUTH_URL, headers=AUTH_HEADER)
    if response.status_code == 200:
        return response.text.strip()  # Return the plain text token
    else:
        raise ValueError(f"Authentication failed: {response.status_code}, {response.text}")


def fetch_client_data(client_id):
    """Generate a token and send a request to the client-registry API for a specific ID."""
    try:
        token = authenticate()  # Generate a token for this request
        headers = {
            'Authorization': f'Bearer {token}',
        }
        params = {
            'custom_validation_payload': 1,
            'dynamic_id_search': 1,
            'agent': 'EP-00025',
            'id': client_id
        }
        response = requests.get(FETCH_URL, headers=headers, params=params)
        if response.status_code == 200:
            data = response.json()
            message = data.get("message", {})
            total = message.get("total", 0)
            
            if total == 0:
                error_message = "No data found for this ID."
                return client_id, response.status_code, None, None, error_message, data

            # Extract necessary fields from the response
            result = message.get("result", [{}])
            if result and isinstance(result, list):
                premium_amount = result[0].get("meansTestingResults", {}).get("premiumAmount", None)
                citizen_client_registry_number = result[0].get("citizenClientRegistryNumber", None)
            else:
                premium_amount = None
                citizen_client_registry_number = None
            return client_id, response.status_code, premium_amount, citizen_client_registry_number, None, data
        else:
            return client_id, response.status_code, None, None, f"Error {response.status_code}", {}
    except Exception as e:
        print(f"Error during request for ID {client_id}: {e}")
        return client_id, None, None, None, f"Request failed: {str(e)}", {}


def process_ids_in_parallel(ids, json_output_path, csv_output_path):
    """Process IDs in parallel and save results to JSON and CSV."""
    global counter
    total_ids = len(ids)  # Total number of IDs to process
    responses = []  # To store all responses for JSON export
    with open(csv_output_path, 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        # Write CSV header
        csv_writer.writerow(['ID', 'Status', 'PremiumAmount', 'CitizenClientRegistryNumber', 'Error', 'Response'])

        # Use ThreadPoolExecutor for parallel processing
        with ThreadPoolExecutor(max_workers=THREAD_COUNT) as executor:
            future_to_id = {executor.submit(fetch_client_data, client_id): client_id for client_id in ids}

            for future in as_completed(future_to_id):
                try:
                    client_id, status, premium_amount, citizen_client_registry_number, error_message, response = future.result()
                    
                    # Update counter safely
                    with counter_lock:
                        counter += 1
                        print(f"Processed {counter}/{total_ids} items - ID: {client_id} - Status: {status}, PremiumAmount: {premium_amount}, CitizenClientRegistryNumber: {citizen_client_registry_number}, Error: {error_message}")

                    # Save to CSV
                    csv_writer.writerow([client_id, status, premium_amount, citizen_client_registry_number, error_message, json.dumps(response)])
                    # Save to JSON
                    responses.append({
                        "ID": client_id,
                        "Status": status,
                        "PremiumAmount": premium_amount,
                        "CitizenClientRegistryNumber": citizen_client_registry_number,
                        "Error": error_message,
                        "Response": response
                    })
                    # Rate limiting (within each thread)
                    time.sleep(CALL_DELAY)
                except Exception as e:
                    print(f"Error processing ID: {future_to_id[future]}. Error: {e}")

    # Write to JSON file
    with open(json_output_path, 'w') as jsonfile:
        json.dump(responses, jsonfile, indent=4)


if __name__ == "__main__":
    # Input file
    file_path = "ids.csv"  # Replace with your file path
    # Output files
    json_output_path = "responses.json"
    csv_output_path = "responses.csv"

    # Read IDs from file
    with open(file_path, 'r') as file:
        ids = [row[0] for row in csv.reader(file)]

    # Process IDs in parallel
    process_ids_in_parallel(ids, json_output_path, csv_output_path)
