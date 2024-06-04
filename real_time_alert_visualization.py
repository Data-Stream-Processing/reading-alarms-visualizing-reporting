from kafka import KafkaConsumer
import json
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation

class AnomalyAlert:
    def __init__(self, id, value, timestamp):
        self.id = id
        self.value = value
        self.timestamp = timestamp

def deserialize_alert(message):
    data = json.loads(message)
    return AnomalyAlert(data['id'], data['value'], data['timestamp'])

def update(frame, consumer, alerts, ax):
    for message in consumer:
        alert_data = message.value
        alert = deserialize_alert(alert_data)
        alerts.append(alert)
        print(f"Received alert: {alert_data}")
        break  # Process one message per frame update

    ax.clear()
    alert_values = [alert.value for alert in alerts]
    ax.hist(alert_values, bins=20, edgecolor='black')
    ax.set_xlabel('Alert Value')
    ax.set_ylabel('Frequency')
    ax.set_title('Distribution of Alert Values')

def consume_alerts():
    consumer = KafkaConsumer(
        'test-alert',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='alert-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    alerts = []

    fig, ax = plt.subplots()
    ani = FuncAnimation(fig, update, fargs=(consumer, alerts, ax), interval=1000)

    plt.show()

if __name__ == "__main__":
    consume_alerts()
