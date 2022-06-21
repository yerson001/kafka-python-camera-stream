import time
import sys
import cv2

from kafka import KafkaProducer
from kafka.errors import KafkaError

producer = KafkaProducer(bootstrap_servers='localhost:9092')
topic = 'my-topic'


def emit_video():
    print('start')

    video = cv2.VideoCapture(0)

    while video.isOpened():
        success, frame = video.read()
        if not success:
            break

        data = cv2.imencode('.jpeg', frame)[1].tobytes()

        future = producer.send(topic, data)
        try:
            future.get(timeout=10)
        except KafkaError as e:
            print(e)
            break

        print('.', end='', flush=True)

emit_video()
