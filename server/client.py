mapping = {
        "request_id": "125",
        "src_key": "test_live_portrait/s5.jpg",
        "driving_key": "test_live_portrait/d13.mp4",
    }

import requests, json

base_url = "http://localhost:5000"
headers = {"accept": "application/json", "Content-Type": "application/json"}

response = requests.post(f"{base_url}/submit", headers=headers, json=mapping)
print(response)
