import requests
import pandas as pd
import datetime

# פנייה ל-API של ה-Datastore לשליפת נתוני JSON (יציב יותר מהורדה ישירה)
url = f"[https://data.gov.il/api/3/action/datastore_search?resource_id=](https://data.gov.il/api/3/action/datastore_search?resource_id=){resource_id}&limit=32000"

try:
    print(f"--- Starting Data Fetch: {datetime.datetime.now()} ---")
    
    # Headers המדמים דפדפן אנושי כדי למנוע חסימות WAF
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    
    response = requests.get(url, headers=headers, timeout=60)
    
    if response.status_code == 200:
        data = response.json()
        records = data.get('result', {}).get('records', [])
        
        if records:
            # יצירת DataFrame מהרשומות שהתקבלו
            df = pd.DataFrame(records)
            
            # סינון עבור תל אביב-יפו (השם הרשמי במאגר)
            # אנחנו משתמשים ב-contains כדי לוודא שנתפוס את כל הווריאציות של השם
            df_tlv = df[df['שם יישוב'].astype(str).str.contains('תל אביב', na=False)].copy()
            
            if not df_tlv.empty:
                # שמירת הקובץ בפורמט CSV עם BOM לעברית תקינה
                output_file = "tlv_deals_master.csv"
                df_tlv.to_csv(output_file, index=False, encoding='utf-8-sig')
                print(f"✅ Success! Saved {len(df_tlv)} transactions to {output_file}")
            else:
                print("⚠️ No Tel Aviv transactions found in this batch.")
        else:
            print("❌ API returned successfully but contained no records.")
    else:
        print(f"❌ Server Error: {response.status_code}")
        sys.exit(1)
        
except Exception as e:
    print(f"❌ Critical Error during execution: {e}")
    sys.exit(1)

if __name__ == "__main__":
    fetch_government_data()
