#!/usr/bin/env python3
"""
========================================================================================
🚀 ALL-IN-ONE AUTOMATION SUITE: DATA INGESTION, TRANSFORMATION & EXECUTIVE REPORTING
========================================================================================
Description:
    A complete, standalone automation script for business transaction reconciliation.
    It ingests dirty multi-source retail CSV/JSON files, normalizes inconsistent date/currency
    formats, deduplicates records via cryptographic hashing, detects fraud/anomalies,
    and automatically generates warehouse-ready CSV datasets, an executive Markdown report,
    and a modern standalone interactive HTML dashboard.

Zero Dependencies:
    Uses 100% Python 3 Standard Library only (no pip install required).
========================================================================================
"""

import os
import sys
import csv
import json
import re
import hashlib
import time
from datetime import datetime
from pathlib import Path

# --- DIRECTORY SETUP ---
BASE_DIR = Path(__file__).resolve().parent if "__file__" in locals() else Path.cwd()
RAW_DIR = BASE_DIR / "data" / "raw"
PROCESSED_DIR = BASE_DIR / "data" / "processed"
REPORTS_DIR = BASE_DIR / "reports"

for folder in [RAW_DIR, PROCESSED_DIR, REPORTS_DIR]:
    folder.mkdir(parents=True, exist_ok=True)

# --- CONFIGURATION & RULES ---
CONFIG = {
    "supported_categories": [
        "Electronics", "Apparel", "Home & Kitchen", 
        "Books & Media", "Sports & Outdoors", "Health & Beauty"
    ],
    "category_aliases": {
        "elec": "Electronics", "electronic": "Electronics", "tech": "Electronics",
        "clothing": "Apparel", "wear": "Apparel",
        "home": "Home & Kitchen", "kitchen": "Home & Kitchen",
        "books": "Books & Media", "media": "Books & Media",
        "sports": "Sports & Outdoors", "fitness": "Sports & Outdoors",
        "health": "Health & Beauty", "beauty": "Health & Beauty"
    },
    "anomaly_thresholds": {
        "high_order_value_usd": 2000.00,
        "excessive_quantity_units": 50,
        "high_branch_refund_pct": 15.0
    },
    "standard_tax_rate": 0.08
}

# ======================================================================================
# 1. MOCK DATA GENERATOR (Creates realistic raw files if none exist)
# ======================================================================================
def generate_sample_data():
    print("[1/5] Synthesizing realistic raw batch data...")
    
    # Raw Sales Batch 1 (North Region - Messy dates, currency signs, duplicate rows)
    north_csv = """Order_ID,Customer_ID,Date,Product_Name,Category,Unit_Price,Quantity,Cost_Price,Customer_Segment,Branch
ORD-1001,CUST-501,2026-11-01,Quantum 4K Smart Monitor,Electronics,$450.00,2,$320.00,Enterprise,North
ORD-1002,CUST-502,01/11/2026,Ergonomic Mechanical Keyboard,elec,$120.00,3,$70.00,,North
ORD-1003,CUST-503,2026/11/02,Noise-Cancelling Headphones,  Electronics  ,$280.00,1,$150.00,Retail,North
ORD-1004,CUST-504,Nov 02 2026,Wireless Vertical Mouse,electronic,  $65.50  ,5,$30.00,Retail,North
ORD-1005,CUST-505,2026-11-03,Enterprise Server Rack Unit,tech,$2850.00,1,$2100.00,Enterprise,North
ORD-1006,CUST-506,2026-11-03,Organic Cotton Hoodie,Clothing,$85.00,4,$35.00,Retail,North
ORD-1002,CUST-502,01/11/2026,Ergonomic Mechanical Keyboard,elec,$120.00,3,$70.00,,North
ORD-1007,CUST-507,2026-11-04,Smart Air Purifier Pro,home,$299.00,2,$190.00,VIP,North
ORD-CORRUPT-1,,2026-11-04,Bad Row Missing Customer,elec,$100.00,1,$80.00,Retail,North
ORD-1008,CUST-508,2026-11-05,Ceramic Non-Stick Pan Set,kitchen,$145.00,-2,$90.00,Retail,North"""
    
    with open(RAW_DIR / "sales_north.csv", "w", encoding="utf-8") as f:
        f.write(north_csv.strip())

    # Raw Sales Batch 2 (South Region - Alternative column names)
    south_csv = """order_id,cust_id,order_date,item_name,department,unit_rate,qty,unit_cost,tier,branch_name
ORD-2001,CUST-601,2026-11-01,4K OLED Cinema TV 65",Electronics,$1899.99,1,$1400.00,VIP,South
ORD-2002,CUST-602,2026-11-02,Trail Running Shoes,sports,$130.00,2,$65.00,Standard,South
ORD-2003,CUST-603,02-11-2026,Stainless Thermal Tumbler,Home & Kitchen,$35.00,6,$12.00,,South
ORD-2004,CUST-604,2026-11-03,Full Stack Engineering Guide,Books & Media,$59.90,3,$25.00,Standard,South
ORD-2005,CUST-605,2026-11-03,Hydrating Facial Serum,health,$42.00,4,$18.00,Retail,South
ORD-2006,CUST-606,2026-11-04,Yoga Mat Pro Eco,fitness,$55.00,3,$22.00,Standard,South
ORD-2007,CUST-607,2026-11-04,Cold Brew Coffee Maker,kitchen,$79.00,2,$40.00,VIP,South
ORD-2008,CUST-608,2026-11-05,Gaming Graphics Card RTX,tech,$1450.00,2,$1150.00,Enterprise,South"""
    
    with open(RAW_DIR / "sales_south.csv", "w", encoding="utf-8") as f:
        f.write(south_csv.strip())

    # Customer Returns Batch (JSON format)
    returns_data = [
        {"return_id": "RET-01", "order_id": "ORD-1003", "refund_amount": 280.00, "reason": "Defective audio driver"},
        {"return_id": "RET-02", "order_id": "ORD-2006", "refund_amount": 165.00, "reason": "Incorrect color delivered"}
    ]
    with open(RAW_DIR / "returns.json", "w", encoding="utf-8") as f:
        json.dump(returns_data, f, indent=2)

    print("   ✓ Created sample CSV and JSON files in data/raw/")


# ======================================================================================
# 2. DATA CLEANING & TRANSFORMATION HELPERS
# ======================================================================================
def standardize_date(raw_date):
    if not raw_date: return None
    s = str(raw_date).strip()
    m1 = re.match(r"^(\d{4})[-/](\d{1,2})[-/](\d{1,2})", s)
    if m1:
        y, m, d = m1.groups()
        return f"{y}-{int(m):02d}-{int(d):02d}"
    m2 = re.match(r"^(\d{1,2})[-/](\d{1,2})[-/](\d{4})", s)
    if m2:
        d, m, y = m2.groups()
        return f"{y}-{int(m):02d}-{int(d):02d}"
    for fmt in ("%b %d %Y", "%B %d %Y", "%d %b %Y", "%Y-%m-%d"):
        try: return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError: pass
    return None

def clean_currency(val):
    if val is None: return 0.0
    cleaned = re.sub(r"[\$,\s]", "", str(val))
    try: return float(cleaned)
    except ValueError: return 0.0

def normalize_category(raw_cat):
    if not raw_cat: return "Uncategorized"
    lower = raw_cat.strip().lower()
    if lower in CONFIG["category_aliases"]:
        return CONFIG["category_aliases"][lower]
    for c in CONFIG["supported_categories"]:
        if c.lower() == lower: return c
    return raw_cat.strip()


# ======================================================================================
# 3. CORE PROCESSING PIPELINE
# ======================================================================================
def run_pipeline():
    start_time = time.time()
    print("\n" + "="*65)
    print("🚀 STARTING AUTOMATED DATAFLOW PIPELINE")
    print("="*65)

    raw_files = [f for f in os.listdir(RAW_DIR) if not f.startswith(".")]
    if not raw_files:
        generate_sample_data()
        raw_files = [f for f in os.listdir(RAW_DIR) if not f.startswith(".")]

    # Load returns
    returns_map = {}
    returns_file = next((f for f in raw_files if "return" in f and f.endswith(".json")), None)
    if returns_file:
        with open(RAW_DIR / returns_file, "r", encoding="utf-8") as f:
            for item in json.load(f):
                if "order_id" in item: returns_map[item["order_id"]] = item

    print(f"[2/5] Ingesting and validating incoming files...")
    transactions = []
    seen_hashes = set()
    corrupt_count = 0
    duplicate_count = 0
    anomalies = []

    for file_name in [f for f in raw_files if f.endswith(".csv")]:
        with open(RAW_DIR / file_name, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            reader.fieldnames = [h.strip().lower() for h in reader.fieldnames]

            for row in reader:
                order_id = row.get("order_id") or row.get("order id") or row.get("order_no") or ""
                cust_id = row.get("customer_id") or row.get("cust_id") or ""
                raw_date = row.get("date") or row.get("order_date") or ""
                product = row.get("product_name") or row.get("item_name") or ""
                raw_cat = row.get("category") or row.get("department") or ""
                raw_price = row.get("unit_price") or row.get("unit_rate") or ""
                raw_qty = row.get("quantity") or row.get("qty") or ""
                raw_cost = row.get("cost_price") or row.get("unit_cost") or ""
                branch = row.get("branch") or row.get("branch_name") or "General"

                # Validation
                if not order_id or not cust_id or "CORRUPT" in order_id:
                    corrupt_count += 1
                    continue
                try:
                    qty = int(raw_qty)
                    price = clean_currency(raw_price)
                    cost = clean_currency(raw_cost) or round(price * 0.7, 2)
                except ValueError:
                    corrupt_count += 1
                    continue
                if qty <= 0 or price <= 0:
                    corrupt_count += 1
                    continue
                std_date = standardize_date(raw_date)
                if not std_date:
                    corrupt_count += 1
                    continue

                # Deduplication via hash
                h = hashlib.md5(f"{order_id}_{cust_id}_{product}_{price}_{qty}".encode()).hexdigest()
                if h in seen_hashes:
                    duplicate_count += 1
                    continue
                seen_hashes.add(h)

                # Calculations
                gross = round(price * qty, 2)
                total_cost = round(cost * qty, 2)
                tax = round(gross * CONFIG["standard_tax_rate"], 2)
                is_ret = order_id in returns_map
                refund = returns_map[order_id]["refund_amount"] if is_ret else 0.0
                net = round(gross - refund, 2)
                profit = round(net - total_cost, 2)
                margin = round((profit / net) * 100, 2) if net > 0 else 0.0

                if gross >= CONFIG["anomaly_thresholds"]["high_order_value_usd"]:
                    anomalies.append(f"High-Value Order: {order_id} (${gross:,.2f})")

                transactions.append({
                    "order_id": order_id, "customer_id": cust_id, "date": std_date,
                    "branch": branch.strip(), "product_name": product.strip(),
                    "category": normalize_category(raw_cat), "unit_price": price,
                    "quantity": qty, "gross_revenue": gross, "tax_amount": tax,
                    "is_returned": is_ret, "refund_amount": refund, "net_revenue": net,
                    "net_profit": profit, "profit_margin_pct": margin
                })

    transactions.sort(key=lambda x: x["date"])
    print(f"   ✓ Valid Transactions: {len(transactions)} | Duplicates Dropped: {duplicate_count} | Corrupt Dropped: {corrupt_count}")

    # Aggregations
    print(f"[3/5] Calculating KPIs and analytical aggregates...")
    gross_total = sum(t["gross_revenue"] for t in transactions)
    net_total = sum(t["net_revenue"] for t in transactions)
    profit_total = sum(t["net_profit"] for t in transactions)
    refund_total = sum(t["refund_amount"] for t in transactions)
    units_total = sum(t["quantity"] for t in transactions)
    overall_margin = round((profit_total / net_total) * 100, 2) if net_total > 0 else 0.0
    aov = round(net_total / len(transactions), 2) if transactions else 0.0

    # Category breakdown
    cat_summary = {}
    for t in transactions:
        c = t["category"]
        if c not in cat_summary: cat_summary[c] = {"net": 0.0, "units": 0, "orders": 0}
        cat_summary[c]["net"] += t["net_revenue"]
        cat_summary[c]["units"] += t["quantity"]
        cat_summary[c]["orders"] += 1

    # Export Processed CSV
    print(f"[4/5] Exporting warehouse-ready clean dataset...")
    csv_out = PROCESSED_DIR / "cleaned_transactions.csv"
    with open(csv_out, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(transactions[0].keys()))
        writer.writeheader()
        writer.writerows(transactions)

    # Export Reports
    print(f"[5/5] Compiling Markdown and interactive HTML reports...")
    
    # 1. Markdown Report
    md_report = f"""# 📊 Executive Business Intelligence Report
*Automated Transaction Reconciliation & Operations Summary*

**Generated At:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  
**Status:** ✅ Successfully Processed

---

## 🚀 Key Performance Indicators (Scorecard)
| Metric | Value | Note |
| :--- | :--- | :--- |
| **Gross Invoiced Sales** | **${gross_total:,.2f}** | Total billings |
| **Net Realized Revenue** | **${net_total:,.2f}** | After ${refund_total:,.2f} refunds |
| **Net Operating Profit** | **${profit_total:,.2f}** | Realized earnings |
| **Overall Profit Margin** | **{overall_margin}%** | Operational health |
| **Orders Fulfilled** | **{len(transactions)}** orders | Validated |
| **Average Order Value (AOV)** | **${aov:,.2f}** | Per-order average |
| **Units Moved** | **{units_total}** items | Physical volume |

---

## 📦 Category Performance
| Category | Orders | Units Sold | Net Sales |
| :--- | :---: | :---: | :---: |
"""
    for cat, d in cat_summary.items():
        md_report += f"| **{cat}** | {d['orders']} | {d['units']} | ${d['net']:,.2f} |\n"

    with open(REPORTS_DIR / "executive_summary.md", "w", encoding="utf-8") as f:
        f.write(md_report)

    # 2. Modern HTML Dashboard
    html_report = f"""<!DOCTYPE html>
<html>
<head>
    <title>DataFlow Executive Dashboard</title>
    <style>
        body {{ font-family: -apple-system, sans-serif; background: #0f172a; color: #f8fafc; padding: 30px; }}
        .card-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 16px; margin: 20px 0; }}
        .card {{ background: #1e293b; padding: 20px; border-radius: 12px; border: 1px solid #334155; }}
        .val {{ font-size: 26px; font-weight: 700; color: #38bdf8; margin-top: 8px; }}
        table {{ width: 100%; border-collapse: collapse; margin-top: 20px; }}
        th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #334155; font-size: 14px; }}
        th {{ background: #1e293b; color: #94a3b8; }}
    </style>
</head>
<body>
    <h1>🚀 Executive Automation Dashboard</h1>
    <p style="color:#94a3b8;">Automated Financial Reconciliation & Ingestion Pipeline</p>
    <div class="card-grid">
        <div class="card"><div>Gross Sales</div><div class="val">${gross_total:,.2f}</div></div>
        <div class="card"><div>Net Revenue</div><div class="val" style="color:#34d399;">${net_total:,.2f}</div></div>
        <div class="card"><div>Net Profit</div><div class="val" style="color:#a78bfa;">${profit_total:,.2f}</div></div>
        <div class="card"><div>Profit Margin</div><div class="val">{overall_margin}%</div></div>
        <div class="card"><div>Orders / AOV</div><div class="val">{len(transactions)} (${aov:,.2f})</div></div>
    </div>
    <h2>📋 Cleaned Transactions (Sample)</h2>
    <table>
        <tr><th>Order ID</th><th>Date</th><th>Branch</th><th>Product</th><th>Category</th><th>Qty</th><th>Net Amount</th></tr>
        {''.join(f"<tr><td><strong>{t['order_id']}</strong></td><td>{t['date']}</td><td>{t['branch']}</td><td>{t['product_name']}</td><td>{t['category']}</td><td>{t['quantity']}</td><td>${t['net_revenue']:,.2f}</td></tr>" for t in transactions[:8])}
    </table>
</body>
</html>"""

    with open(REPORTS_DIR / "executive_dashboard.html", "w", encoding="utf-8") as f:
        f.write(html_report)

    duration = int((time.time() - start_time) * 1000)
    print("\n" + "="*65)
    print(f"🎉 PIPELINE COMPLETED SUCCESSFULLY IN {duration}ms!")
    print(f"   Outputs: data/processed/cleaned_transactions.csv")
    print(f"            reports/executive_summary.md")
    print(f"            reports/executive_dashboard.html")
    print("="*65 + "\n")


if __name__ == "__main__":
    run_pipeline()
