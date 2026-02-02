import requests
import pandas as pd
import datetime

def fetch_government_data():
    # זהו המזהה המעודכן ביותר למאגר העסקאות (2022-2026)
    resource_id = "d3fdc35d-b1df-4ffd-96a1-067856b3e230"
    url = f"https://data.gov.il/api/3/action/datastore_search?resource_id={resource_id}&limit=30000"
    
    try:
        print(f"Connecting to official Datastore at {datetime.datetime.now()}")
        # הוספת Headers כדי לדמות דפדפן ולמנוע חסימת בוטים
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers, timeout=60)
        
        if response.status_code == 200:
            records = response.json()['result']['records']
            df = pd.DataFrame(records)
            
            # ניקוי וסינון תל אביב (השם הרשמי במאגר: 'תל אביב -יפו')
            df_tlv = df[df['שם יישוב'].str.contains('תל אביב', na=False)]
            
            if not df_tlv.empty:
                df_tlv.to_csv("tlv_deals_master.csv", index=False, encoding='utf-8-sig')
                print(f"✅ Success! Found {len(df_tlv)} deals in Tel Aviv.")
            else:
                print("⚠️ Connection ok, but no Tel Aviv records found in this batch.")
        else:
            print(f"❌ Failed with status: {response.status_code}. The resource might be temporarily offline.")
            exit(1)
    except Exception as e:
        print(f"❌ Critical Error: {e}")
        exit(1)

if __name__ == "__main__":
    fetch_government_data()
