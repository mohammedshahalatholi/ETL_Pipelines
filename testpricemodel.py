import psycopg2
import sys
from datetime import datetime

# === DB Configuration ===
DB_CONFIG = {
    "host": "localhost",
    "dbname": "postgres",
    "user": "postgres",
    "password": "admin"
}

# === RU Base Pricing Fallback ===
def RU_basepricing(application_group):
    print(f"[INFO] PXQ Pricing for: {application_group}")
    # Static or calculated fallback yearly price
    # yearly_price = 12000
    # reporting(application_group, yearly_price)

# === Matrix Pricing Logic ===
def matrix_pricing(application_group, year_input):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute("""
        SELECT price_year1, price_year2 FROM pricingmatrix_table
        WHERE application_group_name = %s
    """, (application_group,))
    row = cur.fetchone()

    if not row:
        print(f"[WARNING] No matrix data for: {application_group}")
        return

    price_year1, price_year2 = row
    # testyear=2025
    # current_year = datetime.now().year
    # previous_year = current_year - 1

    if int(year_input) == 2022:
        yearly_price = price_year1
        print(f"[INFO] Year matched price_year1: {yearly_price}")
        reporting(application_group, yearly_price)
    elif int(year_input) == 2023:
        yearly_price = price_year2
        print(f"[INFO] Year matched price_year2: {yearly_price}")
        reporting(application_group, yearly_price)
    else:
        print(f"[WARNING] Year {year_input} doesn't match")
        return

    
    cur.close()
    conn.close()

# === Insert Daily and Monthly Prices ===
def reporting(application_group, yearly_price):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # Daily price
    daily_price = yearly_price / 365
    cur.execute("""
        INSERT INTO daily_table (application_group_name, date, price)
        VALUES (%s, CURRENT_DATE, %s)
    """, (application_group, daily_price))

    # Monthly price (only on day 1)
    if datetime.now().day != 1:
        monthly_price = yearly_price / 12
        cur.execute("""
            INSERT INTO monthly_table (application_group_name, month, price)
            VALUES (%s, TO_CHAR(CURRENT_DATE, 'YYYY-MM'), %s)
        """, (application_group, monthly_price))

    conn.commit()
    cur.close()
    conn.close()

    print(f"[SUCCESS] Inserted daily{' and monthly' if datetime.now().day == 1 else ''} pricing for {application_group}")

# === Main Controller ===
def process_pricing_model(year_input):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute("SELECT application_group_name, pricing_model FROM pricingmodel_table")
    rows = cur.fetchall()

    for application_group, pricing_model in rows:
        print(f"[PROCESSING] {application_group} | Model: {pricing_model}")
        if pricing_model == 'PXQ':
            RU_basepricing(application_group)
        else:
            matrix_pricing(application_group, year_input)

    cur.close()
    conn.close()

# === Entry Point ===
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python pricing_script.py <year>")
        sys.exit(1)

    input_year = sys.argv[1]
    process_pricing_model(input_year)
