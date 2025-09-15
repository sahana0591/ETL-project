import pandas as pd

def clean_sales_data(input_file, output_file):
    # Extract
    df = pd.read_csv(input_file)

    # Transform
    # 1. Drop rows with missing customer_id
    df = df.dropna(subset=['customer_id'])

    # 2. Standardize order_date format
    df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')
    df = df.dropna(subset=['order_date'])  # drop rows where parsing failed
    df['order_date'] = df['order_date'].dt.strftime('%Y-%m-%d')

    # 3. Fix negative quantity values
    df['quantity'] = df['quantity'].fillna(0).abs()

    # 4. Add total_price
    df['total_price'] = df['quantity'] * df['price']

    # Load
    df.to_csv(output_file, index=False)
    print(f"✅ Cleaned data written to {output_file}")


if __name__ == "__main__":
    clean_sales_data("data/sales.csv", "data/clean_sales.csv")
