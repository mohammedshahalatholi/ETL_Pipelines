import psycopg2
import cx_Oracle
import sys
from datetime import datetime

# === PostgreSQL Reporting DB Configuration ===
DB_CONFIG = {
    "host": "localhost",
    "dbname": "reporting",
    "user": "postgres",
    "password": "admin"
}

# === PXQ Pricing Logic ===
def RU_basepricing(application_group):
    print(f"[INFO] PXQ Pricing for: {application_group}")

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # Fetch dupricing_model as yearly_price
        cur.execute("""
            SELECT dupricing_model FROM pricingmodel_table
            WHERE application_group_name = %s AND pricing_model = 'PXQ'
        """, (application_group,))
        row = cur.fetchone()

        if not row:
            print(f"[WARNING] No dupricing_model found for PXQ group: {application_group}")
            return

        yearly_price = row[0]
        print(f"[INFO] Fetched yearly_price (dupricing_model): {yearly_price}")

        cur.close()
        conn.close()

        # Execute mapping logic and then insert pricing
        Databasemapping(application_group)
        reporting(application_group, yearly_price)

    except Exception as e:
        print(f"[ERROR] Failed in RU_basepricing for {application_group}: {e}")

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

    if int(year_input) == 2023:
        yearly_price = price_year1  # year 2023
    elif int(year_input) == 2022:
        yearly_price = price_year2  # year 2022
    else:
        print(f"[WARNING] Year {year_input} doesn't match 2022 or 2023")
        return

    print(f"[INFO] Year matched: {year_input} | Price: {yearly_price}")
    reporting(application_group, yearly_price)

    cur.close()
    conn.close()

# === Insert Daily and Monthly Prices ===
def reporting(application_group, yearly_price):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    daily_price = yearly_price / 365
    cur.execute("""
        INSERT INTO daily_table (application_group_name, date, price)
        VALUES (%s, CURRENT_DATE, %s)
    """, (application_group, daily_price))

    if datetime.now().day == 1:
        monthly_price = yearly_price / 12
        cur.execute("""
            INSERT INTO monthly_table (application_group_name, month, price)
            VALUES (%s, TO_CHAR(CURRENT_DATE, 'YYYY-MM'), %s)
        """, (application_group, monthly_price))

    conn.commit()
    cur.close()
    conn.close()

    print(f"[SUCCESS] Inserted daily{' and monthly' if datetime.now().day == 1 else ''} pricing for {application_group}")

# === Fetch driver info ===
def fetch_driver_details(cursor, ref_number):
    cursor.execute("""
        SELECT db_engine, DB_driverpath, db_driverclass, db_username, db_pass
        FROM database_driver_table
        WHERE DBdriverreferncenumber = %s
    """, (ref_number,))
    return cursor.fetchone()

# === Execute External SQL Query ===
def execute_query(engine, driver_path, username, password, query):
    try:
        if engine.lower() == "postgresql":
            host, port, dbname = driver_path.split(":")
            conn = psycopg2.connect(
                host=host,
                port=port,
                dbname=dbname,
                user=username,
                password=password
            )
        elif engine.lower() == "oracle":
            dsn = cx_Oracle.makedsn(*driver_path.split(":", 2))
            conn = cx_Oracle.connect(user=username, password=password, dsn=dsn)
        else:
            print(f"[ERROR] Unsupported database engine: {engine}")
            return

        cur = conn.cursor()
        print(f"[INFO] Executing query:\n{query}")
        cur.execute(query)
        rows = cur.fetchall()
        print(f"[SUCCESS] Retrieved {len(rows)} records.")
        for row in rows:
            print(row)

        cur.close()
        conn.close()

    except Exception as e:
        print(f"[ERROR] Failed to execute query on {engine}: {e}")

# === Mapping driver logic ===
def Databasemapping(application_group):
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        cur.execute("""
            SELECT APP_grp, app_name, DB_name, Environment, DB_driver_refernce_number, SQLquery
            FROM database_table
            WHERE application_group_name = %s
        """, (application_group,))
        rows = cur.fetchall()

        if not rows:
            print(f"[WARN] No mappings found for application group: {application_group}")
            return

        for row in rows:
            APP_grp, app_name, DB_name, Environment, ref_num, SQLquery = row
            print(f"\n[INFO] Processing app: {app_name} ({Environment})")

            driver_details = fetch_driver_details(cur, ref_num)
            if not driver_details:
                print(f"[ERROR] No DB driver found for reference: {ref_num}")
                continue

            engine, path, _, user, pwd = driver_details
            execute_query(engine, path, user, pwd, SQLquery)

        cur.close()
        conn.close()

    except Exception as e:
        print(f"[ERROR] {e}")
        sys.exit(1)

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

    input_year = sys.argv[1]_
