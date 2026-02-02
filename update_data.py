import requests
import pandas as pd
import datetime

def fetch_government_data():
    # ה-Resource ID הרשמי של עסקאות הנדל"ן
    url = "https://data.gov.il/api/3/action/datastore_search?resource_id=d3fdc35d-b1df-4ffd-96a1-067856b3e230&limit=30000"
    
    try:
        print(f"Starting fetch at {datetime.datetime.now()}")
        response = requests.get(url, timeout=60)
        if response.status_code == 200:
            records = response.json()['result']['records']
            df = pd.DataFrame(records)
            
            # סינון תל אביב
            df_tlv = df[df['שם יישוב'].str.contains('תל אביב', na=False)]
            
            # שמירת הקובץ
            df_tlv.to_csv("tlv_deals_master.csv", index=False, encoding='utf-8-sig')
            print(f"Success! {len(df_tlv)} records saved.")
        else:
            print(f"Failed. Status code: {response.status_code}")
            exit(1)
    except Exception as e:
        print(f"Error occurred: {e}")
        exit(1)

if __name__ == "__main__":
    fetch_government_data()
