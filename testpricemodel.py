import psycopg2
from datetime import date

# === Database Configuration ===
DB_CONFIG = {
    "host": "localhost",
    "dbname": "reporting",
    "user": "postgres",
    "password": "admin"
}

# === Entry Function ===
def process_pricing_model():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        cur.execute("""
            SELECT application_grp, pricing_model, PXQ, year
            FROM pricingmodel_table
        """)
        rows = cur.fetchall()

        if not rows:
            print("No records found in pricingmodel_table.")
            return

        for row in rows:
            app_grp, pricing_model, pxq, year = row
            print(f"\nProcessing -> App Group: {app_grp}, Pricing Model: {pricing_model}, PXQ: {pxq}, Year: {year}")

            if pricing_model == pxq:
                print("Calling RU_basepricing...")
                RU_basepricing(app_grp)
            else:
                print("Calling matrix_pricing...")
                matrix_pricing(app_grp, year)

        cur.close()
        conn.close()

    except Exception as e:
        print("Error in process_pricing_model:", e)

# === RU Base Pricing ===
def RU_basepricing(app_group):
    print(f"RU_basepricing called for: {app_group}")
    # base_price = 100  # You can fetch real logic
    # reporting_price(app_group, 1, base_price)
    # reporting_price(app_group, 2, base_price * 1.5)

# === Matrix Pricing Logic ===
def matrix_pricing(app_group, year):
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        cur.execute("""
            SELECT application_group_name, pricing_model
            FROM pricingmatrix_table
            WHERE application_group_name = %s
        """, (app_group,))
        rows = cur.fetchall()

        if not rows:
            print(f"No records found in pricingmatrix_table for {app_group}")
            return

        for row in rows:
            app_grp, pricing_model = row
            price = float(pricing_model)

            if year == 1:
                cur.execute("""
                    UPDATE pricingmatrix_table
                    SET price_year1 = %s
                    WHERE application_group_name = %s
                """, (price, app_grp))
                reporting_price(app_grp, 1, price)

            elif year == 2:
                cur.execute("""
                    UPDATE pricingmatrix_table
                    SET price_year2 = %s
                    WHERE application_group_name = %s
                """, (price, app_grp))
                reporting_price(app_grp, 2, price)

            else:
                print(f"Invalid year: {year}")

            conn.commit()
            print(f"Updated pricingmatrix_table for {app_grp}, Year: {year}, Price: {price}")

        cur.close()
        conn.close()

    except Exception as e:
        print("Error in matrix_pricing:", e)

# === Reporting Price Function ===
def reporting_price(app_group, year, price):
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        today = date.today()
        first_day_of_month = today.replace(day=1)
        monthly_price = price / 12
        daily_price = price / 365
        # Daily insert
        cur.execute("""
            INSERT INTO daily_table (report_date, application_group, price, price_year)
            VALUES (%s, %s, %s, %s)
        """, (today, app_group, round(daily_price, 2), price))

        # Monthly insert
        cur.execute("""
            INSERT INTO monthly_table (report_date, application_group, price, price_year)
            VALUES (%s, %s, %s, %s)
        """, (first_day_of_month, app_group, round(monthly_price, 2), price))

        conn.commit()
        cur.close()
        conn.close()

        print(f"Inserted daily & monthly report for {app_group}, year={year}, price={price}")

    except Exception as e:
        print("Error in reporting_price:", e)

# === Run the Pipeline ===
if __name__ == "__main__":
    process_pricing_model()
