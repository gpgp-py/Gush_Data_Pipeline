import requests
import pandas as pd
import datetime
import sys

# הגדרת קידוד להדפסת עברית ב-Logs של GitHub ללא שגיאות
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')

def fetch_government_data():
    # מזהה המאגר הרשמי של עסקאות הנדל"ן (API יציב)
    resource_id = "d3fdc35d-b1df-4ffd-96a1-067856b3e230"
    url = f"https://data.gov.il/api/3/action/datastore_search?resource_id={resource_id}&limit=32000"
    
    try:
        print(f"--- Starting Data Fetch: {datetime.datetime.now()} ---")
        
        # שימוש ב-User-Agent כדי למנוע חסימה ע"י השרת הממשלתי
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        
        response = requests.get(url, headers=headers, timeout=60)
        
        if response.status_code == 200:
            data = response.json()
            records = data.get('result', {}).get('records', [])
            
            if records:
                # המרת הנתונים ל-DataFrame של Pandas
                df = pd.DataFrame(records)
                
                # סינון: רק עסקאות שקשורות לתל אביב-יפו
                df_tlv = df[df['שם יישוב'].astype(str).str.contains('תל אביב', na=False)].copy()
                
                if not df_tlv.empty:
                    # שמירת הקובץ עם BOM כדי שייפתח תקין באקסל (utf-8-sig)
                    filename = "tlv_deals_master.csv"
                    df_tlv.to_csv(filename, index=False, encoding='utf-8-sig')
                    print(f"✅ Success! Saved {len(df_tlv)} transactions to {filename}")
                else:
                    print("⚠️ No Tel Aviv transactions found in this batch.")
            else:
                print("❌ API connection successful but returned no records.")
        else:
            print(f"❌ Server Error: {response.status_code}")
            sys.exit(1) # יכשיל את ה-Action כדי שנדע שיש בעיה
            
    except Exception as e:
        print(f"❌ Critical Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    fetch_government_data()
