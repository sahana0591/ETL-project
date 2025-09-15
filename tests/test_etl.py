import pandas as pd
from etl import transform

def test_transform_computes_total_and_types():
    df = pd.DataFrame({
        'order_id': ['x1','x2'],
        'order_date': ['2025-01-01','2025-01-02'],
        'customer': ['c1','c2'],
        'product': ['p1','p2'],
        'quantity': ['2','3'],
        'price': ['10.0','20.0']
    })
    out = transform(df)
    assert out['total'].tolist() == [20.0, 60.0]
    assert out['quantity'].dtype == int

def test_transform_handles_bad_date_and_missing_price():
    df = pd.DataFrame({
        'order_id': ['x3'],
        'order_date': ['notadate'],
        'customer': ['c3'],
        'product': ['p3'],
        'quantity': ['1'],
        'price': ['']
    })
    out = transform(df)
    # price empty -> 0.0, total -> 0.0
    assert out.iloc[0]['price'] == 0.0
    assert out.iloc[0]['total'] == 0.0
