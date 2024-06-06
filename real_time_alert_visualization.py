from kafka import KafkaConsumer
import json
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
import threading
import queue

class AnomalyAlert:
    def __init__(self, alert_id, card_id, fraud_type, fraud_details, timestamp):
        self.alert_id = alert_id
        self.card_id = card_id
        self.fraud_type = fraud_type
        self.fraud_details = fraud_details
        self.timestamp = timestamp

    def __repr__(self):
        return f"AnomalyAlert(alert_id={self.alert_id}, card_id={self.card_id}, fraud_type={self.fraud_type}, timestamp={self.timestamp})"

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
    alert_values = [alert.card_id for alert in alerts]
    ax.hist(alert_values, bins=20, edgecolor='black')
    ax.set_xlabel('Card ID')
    ax.set_ylabel('Frequency')
    ax.set_title('Distribution of Card IDs for Alerts')

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
