import requests 
import json 
import os 
from datetime import datetime
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

def fetch_crypto_data():
    url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd&include_last_updated_at=true"

    try:
        response = requests.get(url)
        data = response.json()
        data['scraped_at'] = datetime.now().isoformat()

        # Backup to local JSON
        OUTPUT_FOLDER = "/opt/airflow/data"
        os.makedirs(OUTPUT_FOLDER, exist_ok = True)
        filename = f"bitcoin_{datetime.now().strftime('%Y-%m-%d_%H-%M')}.json"
        with open(os.path.join(OUTPUT_FOLDER, filename), 'w') as f: 
            json.dump(data, f)

        print("Connecting to Snowflake...")
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')

        sql = f"""
                INSERT INTO CRYPTO_DB.RAW.BITCOIN_PRICES (raw_data)
                SELECT PARSE_JSON('{json.dumps(data)}')
        """

        hook.run(sql)
        print("Success! Data loaded into Snowflake.")

    except Exception as e: 
        print(f"Error: {e}")
        raise e 

if __name__ == "__main__":
    fetch_crypto_data()
