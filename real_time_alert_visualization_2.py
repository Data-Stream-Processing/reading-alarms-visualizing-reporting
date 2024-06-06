from kafka import KafkaConsumer
import json
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
import threading
import queue
from datetime import datetime

class AnomalyAlert:
    def __init__(self, alert_id, card_id, fraud_type, fraud_details, timestamp):
        self.alert_id = alert_id
        self.card_id = card_id
        self.fraud_type = fraud_type
        self.fraud_details = fraud_details
        self.timestamp = datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%f')

def deserialize_alert(message):
    data = json.loads(message)
    return AnomalyAlert(
        data['alertId'],
        data['cardId'],
        data['fraudType'],
        data['fraudDetails'],
        data['timestamp']
    )

def update(frame, alert_queue, alerts, ax):
    while not alert_queue.empty():
        alert = alert_queue.get()
        alerts.append(alert)
        print(f"Received alert: {alert}")

    ax.clear()
    timestamps = [alert.timestamp for alert in alerts]
    values = [alert.card_id for alert in alerts]
    if timestamps:
        ax.plot(timestamps, values, marker='o')
        ax.set_xlabel('Time')
        ax.set_ylabel('Card ID / Transaction Value')
        ax.set_title('Transaction Values Over Time')

def kafka_consumer_thread(alert_queue):
    consumer = KafkaConsumer(
        'test-alert',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='alert-group',
        value_deserializer=lambda x: x.decode('utf-8')
    )

    for message in consumer:
        alert = deserialize_alert(message.value)
        alert_queue.put(alert)

def consume_alerts():
    alert_queue = queue.Queue()
    alerts = []

    consumer_thread = threading.Thread(target=kafka_consumer_thread, args=(alert_queue,))
    consumer_thread.daemon = True
    consumer_thread.start()

    fig, ax = plt.subplots()
    ani = FuncAnimation(fig, update, fargs=(alert_queue, alerts, ax), interval=1000)
    plt.show()

if __name__ == "__main__":
    consume_alerts()
