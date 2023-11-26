import os
import json
import requests
import re
from datetime import datetime
from dotenv import load_dotenv


def json_serializer(obj: datetime) -> str:
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError("Type not serializable")

def post(method: str, url: str, data: dict, target: str):
    match target:
        case "kafka":
            headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
            payload = json.dumps({"records": [{"value": data}]}, default=json_serializer)
        case "kinesis":
            stream = re.search(r'/([^/]+)/[^/]*$', url).group(1)
            headers = {'Content-Type': 'application/json'}
            payload = json.dumps({
                "StreamName": stream,
                "Data": data,
                "PartitionKey": stream[23:]    
            }, default=json_serializer)
    return requests.request(method, url, headers=headers, data=payload)

load_dotenv()
api_url = os.getenv('API_URL')

class BatchIngestor:
    def to_msk(self, pin_result, geo_result, user_result) -> None:
        responses = {
            "pin_response": post("POST", f"{api_url}/topics/0ea903d23769.pin", pin_result, "kafka"),
            "geo_response": post("POST", f"{api_url}/topics/0ea903d23769.geo", geo_result, "kafka"),
            "user_response": post("POST", f"{api_url}/topics/0ea903d23769.user", user_result, "kafka"),
        }

        for key, response in responses.items():
            print(f"{key}: {response.status_code}\n")


class StreamIngestor:
    def to_kinesis(self, pin_result, geo_result, user_result):
        responses = {
            "pin_response": post("PUT", f"{api_url}/streams/streaming-0ea903d23769-pin/record", pin_result, "kinesis"),
            "geo_response": post("PUT", f"{api_url}/streams/streaming-0ea903d23769-geo/record", geo_result, "kinesis"),
            "user_response": post("PUT", f"{api_url}/streams/streaming-0ea903d23769-user/record", user_result, "kinesis"),
        }

        for key, response in responses.items():
            print(f"{key}: {response.status_code}\n")

