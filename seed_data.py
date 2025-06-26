import mysql.connector
from dotenv import load_dotenv
import os

# Load environment variables from .env
load_dotenv()

db_config = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME')
}

# Define synthetic data entries
data = [
    ("Open Data Standards", "Data", "Standardising open government data for better reuse.", "https://example.com/data"),
    ("Accessibility First", "UX", "Prioritising accessibility in all public-facing services.", "https://example.com/access"),
    ("Carbon Audit", "Sustainability", "Measuring service emissions during procurement.", "https://example.com/carbon"),
    ("GOV.UK Design System", "Design", "Component-based design for UK public services.", "https://design-system.service.gov.uk"),
    ("Plain English in Policy", "Comms", "Using clear, simple language for public guidance.", "https://example.com/plain"),
    ("Digital Identity Framework", "Security", "Framework for verifying user identity across departments.", "https://example.com/identity"),
    ("Service Blueprints", "UX", "Visualising end-to-end public service delivery.", "https://example.com/blueprints"),
    ("API Gateway Strategy", "Tech", "Standard approach to exposing public APIs securely.", "https://example.com/api"),
    ("Legacy Systems Assessment", "IT", "Evaluating and retiring outdated government tech.", "https://example.com/legacy"),
    ("Ethical AI Usage", "AI", "Guidelines for safe and fair use of AI in services.", "https://example.com/ai")
]

try:
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
    cursor.executemany(
        "INSERT INTO BestPractice (Title, Type, Description, URL) VALUES (%s, %s, %s, %s)",
        data
    )
    conn.commit()
    print(f"Inserted {cursor.rowcount} rows into BestPractice table.")
    cursor.close()
    conn.close()
except Exception as e:
    print(f"Failed to insert data: {e}")
