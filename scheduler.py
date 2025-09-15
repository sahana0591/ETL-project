import subprocess
import schedule
import time

# Function to run ETL
def run_etl():
    subprocess.run(["python", "src/etl.py"], check=True)
    print("ETL job completed.")

# Schedule ETL to run every day at 8 AM
schedule.every().day.at("08:00").do(run_etl)

print("Scheduler started...")

# Keep the scheduler running
while True:
    schedule.run_pending()
    time.sleep(60)  # check every 1 minute
