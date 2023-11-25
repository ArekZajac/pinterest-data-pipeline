
import data_emulation

class BatchIngestion:
    def __init__(self): None

    def to_msk(pin_result, geo_result, user_result):
    api_url = os.getenv('API_URL')

        def json_serializer(obj):
            if isinstance(obj, datetime):
                return obj.isoformat()
            raise TypeError("Type not serializable")

        def post_to_topic(topic, data):
            payload = json.dumps({
                "records": [
                    {   
                    "value": data
                    }
                ]
            }, default=json_serializer)
            headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
            response = requests.post(f"{api_url}/topics/{topic}", data=payload, headers=headers)
            return response
    
    responses = {
        "pin_response": post_to_topic("0ea903d23769.pin", pin_result),
        "geo_response": post_to_topic("0ea903d23769.geo", geo_result),
        "user_response": post_to_topic("0ea903d23769.user", user_result)
    }

    for k, v in responses.items():
        print(f"{k}: {v}\n")