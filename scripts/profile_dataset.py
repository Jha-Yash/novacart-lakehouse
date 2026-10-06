import os
import json
import csv
from collections import Counter, defaultdict

raw_dir = r"c:\Users\yashj\Desktop\hackathon\data\raw"

def profile():
    print("==================================================")
    print("1. CUSTOMERS_CHANGES.CSV")
    print("==================================================")
    with open(os.path.join(raw_dir, 'customers_changes.csv'), mode='r', encoding='utf-8') as f:
        customers = list(csv.DictReader(f))
    print(f"Total records: {len(customers)}")
    cust_ids = [r['customer_id'] for r in customers]
    cust_counts = Counter(cust_ids)
    dup_custs = {k: v for k, v in cust_counts.items() if v > 1}
    null_updated = [r for r in customers if not r['updated_at']]
    print(f"Distinct customer_ids: {len(cust_counts)}")
    print(f"Customer IDs with multiple records (history): {len(dup_custs)}")
    print(f"Records with null updated_at: {len(null_updated)}")
    print(f"Sample customer history (C0004): {[r for r in customers if r['customer_id'] == 'C0004']}")

    print("\n==================================================")
    print("2. FX_RATES.CSV")
    print("==================================================")
    with open(os.path.join(raw_dir, 'fx_rates.csv'), mode='r', encoding='utf-8') as f:
        fx = list(csv.DictReader(f))
    print(f"Total records: {len(fx)}")
    currencies = set(r['currency'] for r in fx)
    min_date = min(r['rate_date'] for r in fx)
    max_date = max(r['rate_date'] for r in fx)
    print(f"Currencies: {currencies}")
    print(f"Date range: {min_date} to {max_date}")

    print("\n==================================================")
    print("3. PRODUCTS.CSV")
    print("==================================================")
    with open(os.path.join(raw_dir, 'products.csv'), mode='r', encoding='utf-8') as f:
        products = list(csv.DictReader(f))
    print(f"Total records: {len(products)}")
    prod_ids = set(r['product_id'] for r in products)
    categories = Counter(r['category'] for r in products)
    print(f"Unique product_ids: {len(prod_ids)}")
    print(f"Categories: {dict(categories)}")

    print("\n==================================================")
    print("4. ORDERS BATCH 1 & 2")
    print("==================================================")
    with open(os.path.join(raw_dir, 'orders_batch_1.csv'), mode='r', encoding='utf-8') as f:
        orders_b1 = list(csv.DictReader(f))
    with open(os.path.join(raw_dir, 'orders_batch_2.csv'), mode='r', encoding='utf-8') as f:
        orders_b2 = list(csv.DictReader(f))
    print(f"Batch 1 row count: {len(orders_b1)}")
    print(f"Batch 2 row count: {len(orders_b2)}")
    
    b1_ids = set(r['order_id'] for r in orders_b1)
    b2_ids = set(r['order_id'] for r in orders_b2)
    overlap_ids = b1_ids.intersection(b2_ids)
    new_in_b2 = b2_ids - b1_ids
    print(f"Unique order_ids in Batch 1: {len(b1_ids)}")
    print(f"Unique order_ids in Batch 2: {len(b2_ids)}")
    print(f"Overlapping order_ids: {len(overlap_ids)}")
    print(f"New order_ids in Batch 2: {len(new_in_b2)}")

    b1_map = {r['order_id']: r for r in orders_b1}
    b2_map = {r['order_id']: r for r in orders_b2}
    changed_status = []
    for oid in overlap_ids:
        r1, r2 = b1_map[oid], b2_map[oid]
        if r1['status'] != r2['status'] or r1['updated_at'] != r2['updated_at']:
            changed_status.append({
                'order_id': oid,
                'b1_status': r1['status'],
                'b2_status': r2['status'],
                'b1_updated_at': r1['updated_at'],
                'b2_updated_at': r2['updated_at']
            })
    print(f"Overlapping orders with changed status/updated_at: {len(changed_status)}")
    if changed_status:
        print(f"Sample changed order: {changed_status[0]}")

    all_orders = orders_b1 + orders_b2
    ts_formats = Counter()
    curr_counts = Counter()
    null_curr = 0
    null_promo = 0
    for r in all_orders:
        ts = r['order_ts']
        if 'T' in ts and 'Z' in ts:
            ts_formats['ISO_UTC_Z'] += 1
        elif 'T' in ts and '+05:30' in ts:
            ts_formats['ISO_TZ_+05:30'] += 1
        elif '/' in ts:
            ts_formats['DD/MM/YYYY_HH:MM'] += 1
        else:
            ts_formats['OTHER'] += 1
        
        c = r['currency']
        if not c:
            null_curr += 1
        else:
            curr_counts[c] += 1
        
        if not r['promo_code']:
            null_promo += 1
    
    print(f"Timestamp formats: {dict(ts_formats)}")
    print(f"Currency counts (raw): {dict(curr_counts)}")
    print(f"Orders with null currency: {null_curr}")
    print(f"Orders with null promo_code: {null_promo}")

    all_known_custs = set(cust_ids)
    unknown_custs_b1 = set(r['customer_id'] for r in orders_b1 if r['customer_id'] not in all_known_custs)
    unknown_custs_b2 = set(r['customer_id'] for r in orders_b2 if r['customer_id'] not in all_known_custs)
    print(f"Batch 1 orders referencing unknown customer_ids: {len(unknown_custs_b1)} distinct IDs ({unknown_custs_b1})")
    print(f"Batch 2 orders referencing unknown customer_ids: {len(unknown_custs_b2)} distinct IDs ({unknown_custs_b2})")

    print("\n==================================================")
    print("5. ORDER_ITEMS BATCH 1 & 2")
    print("==================================================")
    with open(os.path.join(raw_dir, 'order_items_batch_1_json.txt'), mode='r', encoding='utf-8') as f:
        items_b1 = [json.loads(line) for line in f if line.strip()]
    with open(os.path.join(raw_dir, 'order_items_batch_2_jsonl.txt'), mode='r', encoding='utf-8') as f:
        items_b2 = [json.loads(line) for line in f if line.strip()]
    print(f"Items Batch 1 count: {len(items_b1)}")
    print(f"Items Batch 2 count: {len(items_b2)}")

    all_items = items_b1 + items_b2
    keys_b1 = Counter((r['order_id'], r['line_no']) for r in items_b1)
    keys_b2 = Counter((r['order_id'], r['line_no']) for r in items_b2)
    keys_all = Counter((r['order_id'], r['line_no']) for r in all_items)
    
    dup_keys_b1 = {k: v for k, v in keys_b1.items() if v > 1}
    dup_keys_b2 = {k: v for k, v in keys_b2.items() if v > 1}
    dup_keys_cross = {k: v for k, v in keys_all.items() if v > 1}
    print(f"Duplicate (order_id, line_no) within Batch 1: {len(dup_keys_b1)}")
    print(f"Duplicate (order_id, line_no) within Batch 2: {len(dup_keys_b2)}")
    print(f"Duplicate (order_id, line_no) across all items: {len(dup_keys_cross)}")

    line_types = Counter(r.get('line_type') for r in all_items)
    print(f"Line types: {dict(line_types)}")

    neg_qty = [r for r in all_items if int(r.get('qty', 0)) < 0]
    print(f"Items with negative quantity: {len(neg_qty)}")
    print(f"Line types of negative qty: {Counter(r.get('line_type') for r in neg_qty)}")

    null_discounts = sum(1 for r in all_items if r.get('discount_pct') is None)
    discount_types = Counter(type(r.get('discount_pct')).__name__ for r in all_items)
    print(f"Null discount_pct count: {null_discounts}")
    print(f"Discount_pct data types encountered: {dict(discount_types)}")

    unknown_prods = set(r.get('product_id') for r in all_items if r.get('product_id') not in prod_ids)
    print(f"Items referencing product_ids not in products.csv: {unknown_prods}")

    print("\n==================================================")
    print("6. PAYMENTS_JSON.TXT")
    print("==================================================")
    with open(os.path.join(raw_dir, 'payments_json.txt'), mode='r', encoding='utf-8') as f:
        payments = json.load(f)
    print(f"Total payments count: {len(payments)}")
    all_order_ids = b1_ids.union(b2_ids)
    unmatched_payments = [p for p in payments if p['order_id'] not in all_order_ids]
    print(f"Payments whose order_id is not in orders batches: {len(unmatched_payments)}")
    print(f"Sample unmatched payments order_ids: {set(p['order_id'] for p in unmatched_payments)}")
    payment_statuses = Counter(p.get('status') for p in payments)
    print(f"Payment statuses: {dict(payment_statuses)}")
    payment_methods = Counter(p.get('method') for p in payments)
    print(f"Payment methods: {dict(payment_methods)}")

if __name__ == '__main__':
    profile()
