import requests
import time

def fetch_users(api_url, retries=3, timeout=30):
    attempt = 0

    while attempt < retries:
        try:
            response = requests.get(api_url, timeout=timeout)

            if response.status_code == 200:
                return response.json()

            if response.status_code >= 500:
                raise Exception("Server error")

        except Exception as e:
            attempt += 1
            wait = 2 ** attempt
            print(f"Retry {attempt} in {wait} seconds...")
            time.sleep(wait)

    raise Exception("API failed after retries")
