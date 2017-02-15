#!/usr/bin/env python3
"""
Kafka Order Event Producer
Generates realistic order events for streaming ETL testing
"""

import json
import random
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer
from faker import Faker
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class OrderEventGenerator:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.fake = Faker()
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )
        
        # Sample data for realistic generation
        self.products = [
            {"id": "P001", "name": "Laptop Pro", "price": 1299.99, "category": "Electronics"},
            {"id": "P002", "name": "Wireless Mouse", "price": 29.99, "category": "Electronics"},
            {"id": "P003", "name": "Office Chair", "price": 199.99, "category": "Furniture"},
            {"id": "P004", "name": "Coffee Maker", "price": 89.99, "category": "Appliances"},
            {"id": "P005", "name": "Smartphone", "price": 699.99, "category": "Electronics"},
            {"id": "P006", "name": "Desk Lamp", "price": 39.99, "category": "Furniture"},
            {"id": "P007", "name": "Bluetooth Speaker", "price": 79.99, "category": "Electronics"},
            {"id": "P008", "name": "Water Bottle", "price": 19.99, "category": "Lifestyle"},
            {"id": "P009", "name": "Notebook Set", "price": 15.99, "category": "Office"},
            {"id": "P010", "name": "Wireless Charger", "price": 49.99, "category": "Electronics"}
        ]
        
        self.regions = ["North America", "Europe", "Asia Pacific", "Latin America"]
        self.statuses = ["pending", "confirmed", "processing", "shipped", "delivered"]
        self.customer_segments = ["Premium", "Standard", "Basic"]
        
        # Generate customer pool
        self.customers = self._generate_customers(1000)
        
    def _generate_customers(self, count):
        """Generate a pool of customers"""
        customers = []
        for i in range(count):
            customer = {
                "customer_id": f"C{i+1:04d}",
                "customer_name": self.fake.name(),
                "email": self.fake.email(),
                "segment": random.choice(self.customer_segments),
                "registration_date": self.fake.date_time_between(
                    start_date='-2y', end_date='now'
                ).isoformat()
            }
            customers.append(customer)
        return customers
    
    def generate_order_event(self):
        """Generate a realistic order event"""
        customer = random.choice(self.customers)
        product = random.choice(self.products)
        
        # Simulate seasonal patterns and business hours
        now = datetime.now()
        if now.hour < 6 or now.hour > 22:
            # Lower activity during night hours
            if random.random() < 0.3:
                return None
        
        # Generate order with some business logic
        quantity = random.choices([1, 2, 3, 4, 5], weights=[50, 25, 15, 7, 3])[0]
        
        # Simulate pricing variations
        base_price = product["price"]
        price_variation = random.uniform(0.9, 1.1)  # ±10% price variation
        unit_price = round(base_price * price_variation, 2)
        
        order = {
            "order_id": f"ORD{random.randint(100000, 999999)}",
            "customer_id": customer["customer_id"],
            "product_id": product["id"],
            "quantity": quantity,
            "unit_price": unit_price,
            "order_date": now.isoformat(),
            "status": random.choice(self.statuses),
            "region": random.choice(self.regions)
        }
        
        return order, customer
    
    def send_customer_event(self, customer):
        """Send customer data to Kafka"""
        try:
            self.producer.send(
                'customers',
                key=customer["customer_id"],
                value=customer
            )
        except Exception as e:
            logger.error(f"Error sending customer event: {e}")
    
    def send_order_event(self, order):
        """Send order data to Kafka"""
        try:
            self.producer.send(
                'orders',
                key=order["order_id"],
                value=order
            )
            logger.info(f"Sent order: {order['order_id']} - ${order['unit_price'] * order['quantity']:.2f}")
        except Exception as e:
            logger.error(f"Error sending order event: {e}")
    
    def simulate_data_quality_issues(self, order):
        """Occasionally introduce data quality issues for testing"""
        if random.random() < 0.05:  # 5% chance of data issues
            issue_type = random.choice(['negative_quantity', 'zero_price', 'invalid_customer'])
            
            if issue_type == 'negative_quantity':
                order['quantity'] = -1
            elif issue_type == 'zero_price':
                order['unit_price'] = 0
            elif issue_type == 'invalid_customer':
                order['customer_id'] = 'INVALID'
                
        return order
    
    def run_simulation(self, duration_minutes=60, events_per_minute=10):
        """Run the order simulation"""
        logger.info(f"Starting order simulation for {duration_minutes} minutes")
        logger.info(f"Target rate: {events_per_minute} events per minute")
        
        end_time = datetime.now() + timedelta(minutes=duration_minutes)
        
        # Send initial customer data
        logger.info("Sending customer data...")
        for customer in self.customers[:100]:  # Send subset of customers
            self.send_customer_event(customer)
        
        self.producer.flush()
        time.sleep(2)  # Allow customers to be processed
        
        # Start order generation
        logger.info("Starting order generation...")
        event_count = 0
        
        try:
            while datetime.now() < end_time:
                start_minute = time.time()
                
                # Generate events for this minute
                for _ in range(events_per_minute):
                    event_data = self.generate_order_event()
                    if event_data:
                        order, customer = event_data
                        
                        # Occasionally introduce data quality issues
                        order = self.simulate_data_quality_issues(order)
                        
                        # Send events
                        self.send_order_event(order)
                        
                        # Occasionally send customer updates
                        if random.random() < 0.1:
                            self.send_customer_event(customer)
                        
                        event_count += 1
                        
                        # Small delay between events
                        time.sleep(random.uniform(0.1, 0.5))
                
                # Wait for next minute
                elapsed = time.time() - start_minute
                if elapsed < 60:
                    time.sleep(60 - elapsed)
                    
        except KeyboardInterrupt:
            logger.info("Simulation interrupted by user")
        
        finally:
            logger.info(f"Simulation completed. Total events sent: {event_count}")
            self.producer.flush()
            self.producer.close()

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Order Event Generator')
    parser.add_argument('--duration', type=int, default=60, help='Duration in minutes')
    parser.add_argument('--rate', type=int, default=10, help='Events per minute')
    parser.add_argument('--kafka-servers', default='localhost:9092', help='Kafka bootstrap servers')
    
    args = parser.parse_args()
    
    generator = OrderEventGenerator(args.kafka_servers)
    generator.run_simulation(args.duration, args.rate)

if __name__ == "__main__":
    main()