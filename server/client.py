id = "170"

mapping = {
        "request_id": id,
        "src_key": "test_live_portrait/s5.jpg",
        "driving_key": "test_live_portrait/d13.mp4",
    }

import requests, json

api_key = 'oLjQD5hWDYN5DeAQ4cx5CL3vJYOTXf0c'
base_url = 'http://localhost:5000'
headers = {"accept": "application/json", "Content-Type": "application/json", "X-API-Key": api_key}

response = requests.post(f"{base_url}/submit", headers=headers, json=mapping)

while True:
    response = requests.get(f"{base_url}/status/{id}", headers=headers)
    print(response.json())
    if response.json()["status"] == "completed":
        break
    import time
    time.sleep(3)
