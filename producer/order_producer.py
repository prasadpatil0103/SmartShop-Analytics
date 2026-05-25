import pandas as pd
import json
import os
import time
from kafka import KafkaProducer

# ─── Setup ───────────────────────────────────────────
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# ─── Load Olist Data ──────────────────────────────────
print("Loading Olist dataset...")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')

orders = pd.read_csv(os.path.join(DATA_DIR, 'olist_orders_dataset.csv'))
order_items = pd.read_csv(os.path.join(DATA_DIR, 'olist_order_items_dataset.csv'))
payments = pd.read_csv(os.path.join(DATA_DIR, 'olist_order_payments_dataset.csv'))
customers = pd.read_csv(os.path.join(DATA_DIR, 'olist_customers_dataset.csv'))

# ─── Merge into one rich order event ─────────────────
df = orders.merge(order_items, on='order_id', how='left')
df = df.merge(payments, on='order_id', how='left')
df = df.merge(customers, on='customer_id', how='left')

print(f"Total orders loaded: {len(df)}")
print("Starting to stream orders into Kafka...\n")

# ─── Stream each order into Kafka ────────────────────
for index, row in df.iterrows():
    event = {
        "order_id": row['order_id'],
        "customer_id": row['customer_id'],
        "order_status": row['order_status'],
        "order_purchase_timestamp": str(row['order_purchase_timestamp']),
        "payment_type": str(row['payment_type']),
        "payment_value": float(row['payment_value']) if pd.notna(row['payment_value']) else 0.0,
        "product_id": str(row['product_id']),
        "price": float(row['price']) if pd.notna(row['price']) else 0.0,
        "customer_city": str(row['customer_city']),
        "customer_state": str(row['customer_state']),
    }

    # Send to Kafka topic called "orders"
    producer.send('orders', value=event)

    # Print every 100th order so we can see progress
    if index % 100 == 0:
        print(f"✅ Streamed {index} orders into Kafka...")

    # Small delay to simulate real-time streaming
    time.sleep(0.05)

print("\n🎉 All orders streamed successfully!")
producer.flush()
producer.close()