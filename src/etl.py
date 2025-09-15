import os
import pandas as pd
from sqlalchemy import text
from sqlalchemy.types import Integer, String, Numeric, Date
from db import get_engine

# File and table configuration
INPUT_CSV = os.environ.get("INPUT_CSV", "data/sales.csv")
TARGET_TABLE = os.environ.get("TARGET_TABLE", "sales")
STAGING_TABLE = "staging_sales"

def extract(path=INPUT_CSV) -> pd.DataFrame:
    """Read CSV file into a DataFrame"""
    print(f"Extracting from {path}")
    df = pd.read_csv(path)
    return df

def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Clean, type-cast, and prepare DataFrame for loading"""
    df = df.copy()

    # Ensure order_id is string
    df['order_id'] = df['order_id'].astype(str)

    # Parse dates
    df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')

    # Numeric casting
    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').fillna(0).astype(int)
    df['price'] = pd.to_numeric(df['price'], errors='coerce').fillna(0.0)
    df['total'] = df['quantity'] * df['price']

    # Drop rows missing key fields
    df = df.dropna(subset=['order_id'])

    # Ensure column order matches target table
    df = df[['order_id', 'order_date', 'customer', 'product', 'quantity', 'price', 'total']]
    return df

def load(df: pd.DataFrame, engine=None):
    """Load DataFrame into PostgreSQL using staging and upsert strategy"""
    if engine is None:
        engine = get_engine()

    # Ensure order_id is string
    df['order_id'] = df['order_id'].astype(str)

    with engine.begin() as conn:
        print("Writing staging table...")
        df.to_sql(
            STAGING_TABLE,
            conn,
            if_exists='replace',
            index=False,
            dtype={
                "order_id": String(20),      # Important: order_id as string
                "order_date": Date(),
                "customer": String(100),
                "product": String(100),
                "quantity": Integer(),
                "price": Numeric(),
                "total": Numeric(),
            },
        )

        print("Merging staging into target...")
        # Delete duplicates in target that are present in staging
        conn.execute(text(f"""
            DELETE FROM {TARGET_TABLE}
            WHERE order_id IN (SELECT order_id FROM {STAGING_TABLE});
        """))

        # Insert from staging to target
        conn.execute(text(f"""
            INSERT INTO {TARGET_TABLE} (order_id, order_date, customer, product, quantity, price, total)
            SELECT order_id, order_date, customer, product, quantity, price, total
            FROM {STAGING_TABLE};
        """))

        # Drop staging table
        conn.execute(text(f"DROP TABLE IF EXISTS {STAGING_TABLE};"))

    print("Load complete.")

def run():
    df = extract()
    df = transform(df)
    load(df)

if __name__ == "__main__":
    run()
