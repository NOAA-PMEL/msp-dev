from flask import Flask, request, make_response
import uuid

import asynci
import httpx
from logfmter import Logfmter

# from registry import registry
# from flask import Flask, request
# from typing import Dict
# from fastapi import FastAPI, Request
from pydantic import BaseSettings
from cloudevents.http import CloudEvent, from_http
from cloudevents.conversion import to_structured
from cloudevents.exceptions import InvalidStructuredJSON
from time import sleep
import logging
import math

handler = logging.StreamHandler()
handler.setFormatter(Logfmter())
logging.basicConfig(handlers=[handler])
L = logging.getLogger(__name__)
L.setLevel(logging.DEBUG)


app = Flask(__name__)

def time_to_next(sec: float) -> float:
    now = time.time()
    delta = sec - (math.fmod(now, sec))
    return delta

class Tester():
    def __init__(self):
        asyncio.create_task(self.send_message_loop())


    async def send_message_loop(self):
        while True:
            
            data = {"T": 23.5, "RH": 44.3}
            L.info("send_message_loop")# extra={"payload": payload})
            attributes = {
                "type": "test.message.type",
                "source": "test.sender",
                "id": str(ULID()),
                "datacontenttype": "application/json; charset=utf-8",
            }
            
            ce = CloudEvent(attributes=attributes, data=data)

            try:
                headers, body = to_structured(ce)
                # send to knative kafkabroker
                with httpx.Client() as client:
                    r = client.post(
                        "http://broker-ingress.knative-eventing.svc.cluster.local/default/default", headers=headers, data=body
                        # "http://localhost:8080", headers=headers, data=body
                        # config.knative_broker, headers=headers, data=body.decode()
                    )
                    L.info("verifier send", extra={"verifier-request": r.request.content, "status-code": r.status_code})
                    r.raise_for_status()
            except InvalidStructuredJSON:
                L.error(f"INVALID MSG: {ce}")
            except httpx.HTTPError as e:
                L.error(f"HTTP Error when posting to {e.request.url!r}: {e}")

            await asyncio.sleep(time_to_next(1))

test = Tester()


@app.route('/', methods=['POST'])
def hello_world():
    app.logger.warning(request.data)
    # Respond with another event (optional)
    response = make_response({
        "msg": "Hi from helloworld-python app!"
    })
    response.headers["Ce-Id"] = str(uuid.uuid4())
    response.headers["Ce-specversion"] = "0.3"
    response.headers["Ce-Source"] = "knative/eventing/samples/hello-world"
    response.headers["Ce-Type"] = "dev.knative.samples.hifromknative"
    return response

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8080)