
# === Insert Daily and Monthly Prices ===
def sla_reporting(application_group, platform, sla_measured):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    daily_price = sla_measured / 365
    cur.execute("""
        INSERT INTO daily_table (application_group_name, platform, date, price)
        VALUES (%s, CURRENT_DATE, %s)
    """, (application_group,platform, daily_price))

    if datetime.now().day == 1:
        monthly_price = sla_measured / 12
        cur.execute("""
            INSERT INTO monthly_table (application_group_name, platform,month, price)
            VALUES (%s, TO_CHAR(CURRENT_DATE, 'YYYY-MM'), %s)
        """, (application_group,platform, monthly_price))

    conn.commit()
    cur.close()
    conn.close()

    print(f"[SUCCESS] Inserted daily{' and monthly' if datetime.now().day == 1 else ''} pricing for {application_group}")


def fetch_driver_details_sla(cursor, ref_number):
    cursor.execute("""
        SELECT db_engine, DB_driverpath, db_driverclass, db_username, db_pass
        FROM database_driver_table
        WHERE DBdriverreferncenumber = %s
    """, (ref_number,))
    return cursor.fetchone()

# === Execute External SQL Query ===
def execute_query_sla(engine, driver_path, username, password, query, purpose):
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
            app_group, platform, sla_measured = row
            if purpose == 'sla':
                sla_reporting(app_group, platform, sla_measured)

        cur.close()
        conn.close()

    except Exception as e:
        print(f"[ERROR] Failed to execute query on {engine}: {e}")

def sla_pricing(application_group, year_input,purpose):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute("""
        SELECT * from sla_table
        WHERE application_group_name = %s
    """, (application_group,))
    row = cur.fetchall()
    if not row:
        print(f"[WARNING] No matrix data for: {application_group}")
        return
    slameasure,slaunit,slarate,slaname,app_grp,app_name,source=row
    if source=="DB":
        Databasemapping_sla(slarate,app_grp,app_name,source,purpose)
    else:
        filelog_sla(slarate,app_grp,app_name,source,purpose)

    

    cur.close()
    conn.close()

# === Mapping driver logic ===
def Databasemapping_sla(application_group,slarate,app_grp,app_name,source,purpose):
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

            driver_details = fetch_driver_details_sla(cur, ref_num,source)
            if not driver_details:
                print(f"[ERROR] No DB driver found for reference: {ref_num}")
                continue

            engine, path, user, pwd,SQLquery = driver_details
            execute_query_sla(engine, path, user, pwd, SQLquery,source,purpose)

        cur.close()
        conn.close()

    except Exception as e:
        print(f"[ERROR] {e}")
        sys.exit(1)

# === Main Controller ===
def process_sla_model(year_input):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute("SELECT application_group_name, pricing_model, purpose FROM pricingmodel_table")
    rows = cur.fetchall()

    for application_group, pricing_model, purpose in rows:
        print(f"[PROCESSING] {application_group} | Model: {pricing_model} | Purpose: {purpose}")
        sla_pricing(application_group, year_input,purpose)

    cur.close()
    conn.close()


