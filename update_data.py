import requests
import pandas as pd
import sys

# --- כאן נמצאת הגדרת השפה ששאלת עליה ---
# שורה זו מונעת קריסה בעת הדפסת עברית ללוגים
sys.stdout.reconfigure(encoding='utf-8')
# ---------------------------------------

def fetch_data():
    # מזהה המאגר (API)
    resource_id = "d3fdc35d-b1df-4ffd-96a1-067856b3e230"
    url = f"https://data.gov.il/api/3/action/datastore_search?resource_id={resource_id}&limit=32000"
    
    try:
        print("Starting data fetch...")
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers, timeout=60)
        
        if response.status_code == 200:
            records = response.json().get('result', {}).get('records', [])
            if records:
                df = pd.DataFrame(records)
                # סינון תל אביב
                df_tlv = df[df['שם יישוב'].astype(str).str.contains('תל אביב', na=False)].copy()
                
                # שמירה עם קידוד utf-8-sig (כדי שייפתח טוב באקסל בעברית)
                df_tlv.to_csv("tlv_deals_master.csv", index=False, encoding='utf-8-sig')
                print(f"Success! Saved {len(df_tlv)} records.")
            else:
                print("No records found.")
        else:
            print(f"Error: {response.status_code}")
            sys.exit(1)
    except Exception as e:
        print(f"Failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    fetch_data()
