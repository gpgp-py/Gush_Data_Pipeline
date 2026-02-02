import requests
import pandas as pd
import datetime
import sys

# הגדרת תמיכה בעברית להדפסות בטרמינל
sys.stdout.reconfigure(encoding='utf-8')

def fetch_government_data():
    # מזהה המאגר הרשמי של עסקאות הנדל"ן
    resource_id = "d3fdc35d-b1df-4ffd-96a1-067856b3e230"
    url = f"https://data.gov.il/api/3/action/datastore_search?resource_id={resource_id}&limit=32000"
    
    try:
        print(f"--- Starting Data Fetch: {datetime.datetime.now()} ---")
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers, timeout=60)
        
        if response.status_code == 200:
            data = response.json()
            records = data.get('result', {}).get('records', [])
            
            if records:
                df = pd.DataFrame(records)
                # סינון עבור תל אביב-יפו
                df_tlv = df[df['שם יישוב'].astype(str).str.contains('תל אביב', na=False)].copy()
                
                if not df_tlv.empty:
                    df_tlv.to_csv("tlv_deals_master.csv", index=False, encoding='utf-8-sig')
                    print(f"✅ Success! Saved {len(df_tlv)} transactions.")
                else:
                    print("⚠️ No Tel Aviv records found.")
            else:
                print("❌ API returned no records.")
        else:
            print(f"❌ Server Error: {response.status_code}")
            sys.exit(1)
            
    except Exception as e:
        print(f"❌ Critical Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    fetch_government_data()
