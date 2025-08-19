"""config.py contains settings used in the dag"""

daily_sales_schema = [
    {"name": "Date", "type": "DATE"},
    {"name": "Order_id", "type": "STRING"},
    {"name": "transaction_ID", "type": "STRING"},
    {"name": "product", "type": "STRING"},
    {"name": "customer_id", "type": "STRING"},
    {"name": "cost", "type": "FLOAT64"},
]
