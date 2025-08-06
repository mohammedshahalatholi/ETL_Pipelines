import psycopg2
import oracledb
import sys
from datetime import datetime
from sla_reporting import sla_reporting

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

        cur.execute("""
            SELECT * FROM pricingmodel_table
            WHERE application_group_name = %s
        """, (application_group,))
        row = cur.fetchone()
        cur.close()
        conn.close()

        Databasemapping(application_group)

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
        yearly_price = price_year1
    elif int(year_input) == 2022:
        yearly_price = price_year2
    else:
        print(f"[WARNING] Year {year_input} doesn't match 2022 or 2023")
        return

    print(f"[INFO] Year matched: {year_input} | Price: {yearly_price}")
    reporting(application_group, yearly_price)

    cur.close()
    conn.close()

# === Insert Daily and Monthly Prices ===
def RU_reporting(application_group, platform, Ru_measured):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    daily_price = Ru_measured / 365
    cur.execute("""
        INSERT INTO daily_table (application_group_name, date, price)
        VALUES (%s, CURRENT_DATE, %s)
    """, (application_group, daily_price))

    if datetime.now().day == 1:
        monthly_price = Ru_measured / 12
        cur.execute("""
            INSERT INTO monthly_table (application_group_name, month, price)
            VALUES (%s, TO_CHAR(CURRENT_DATE, 'YYYY-MM'), %s)
        """, (application_group, monthly_price))

    conn.commit()
    cur.close()
    conn.close()

    print(f"[SUCCESS] Inserted daily{' and monthly' if datetime.now().day == 1 else ''} pricing for {application_group}")

def reporting(application_group, year_input):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    daily_price = year_input / 365
    cur.execute("""
        INSERT INTO daily_table (application_group_name, date, price)
        VALUES (%s, CURRENT_DATE, %s)
    """, (application_group, daily_price))

    if datetime.now().day == 1:
        monthly_price = year_input / 12
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
def execute_query(engine, driver_path, username, password, query, purpose):
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
            host, port, service = driver_path.split(":")
            dsn = oracledb.makedsn(host, port, service_name=service)
            conn = oracledb.connect(user=username, password=password, dsn=dsn)
        else:
            print(f"[ERROR] Unsupported database engine: {engine}")
            return

        cur = conn.cursor()
        print(f"[INFO] Executing query:\n{query}")
        cur.execute(query)
        rows = cur.fetchall()
        print(f"[SUCCESS] Retrieved {len(rows)} records.")

        for row in rows:
            app_group, platform, RU_measured = row
            if purpose == 'pricing':
                RU_reporting(app_group, platform, RU_measured)
            else:
                sla_reporting(app_group, platform, RU_measured)

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

            engine, path, user, pwd,SQLquery = driver_details
            execute_query(engine, path, user, pwd, SQLquery,)

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

    for application_group, pricing_model, purpose in rows:
        print(f"[PROCESSING] {application_group} | Model: {pricing_model} ")
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
    process_sla_model(input_year)
