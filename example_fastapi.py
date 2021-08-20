import asyncio
import json
from pydantic import BaseModel
import aiohttp
import uvicorn
from fastapi import FastAPI
from fastapi.encoders import jsonable_encoder
from aiokafka_handler.kafka_handler import kafka_consumer, AIOKafkaHandler

"""
# Sending Kafka Messages to an API and vice versa

In this example we create a simple function that forwards messages
from a kafka topic to a rest api.
The function 'kafka_to_http' creates an aiohttp ClientSession and
a Kafka Consumer that posts each message's key to the rest api
using the ClientSession.
The Kafka Consumer is created using the decorator syntax on a function.

On the other side we have a FastAPI endpoint that receives the post requests,
alters the message and writes it to another topic.

The asyncronous function 'kafka_to_http' is created as a background task on
the FastAPI application startup by using 'asyncio.create_task()'
"""

url = "http://localhost:5000/to_kafka"
headers = {"Content-Type": "application/json"}
app = FastAPI()
kh = AIOKafkaHandler()


class Data(BaseModel):
    text: str


async def kafka_to_http():
    async with aiohttp.ClientSession() as s:

        @kafka_consumer("test_input")
        async def post_message(msg=None):
            async with s.post(url, data=msg.value, headers=headers) as resp:
                print(await resp.text())

        await post_message()


@app.post("/to_kafka")
async def to_kafka(data: Data):
    data.text += ". Forwarded by FastAPI"
    await kh.send((None, json.dumps(jsonable_encoder(data))))
    return data


@app.on_event("startup")
async def startup_event():
    await kh.init_producer("fastapi_output_topic")
    asyncio.create_task(kafka_to_http())


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=5000)
