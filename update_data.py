import requests
import pandas as pd
import sys
import datetime

# הגדרת קידוד להדפסת עברית בלוגים
sys.stdout.reconfigure(encoding='utf-8')

def get_dynamic_resource_id():
    """
    פונקציה זו מחפשת באופן אוטומטי את המזהה העדכני של קובץ עסקאות הנדל"ן.
    היא מונעת שגיאות 404 כתוצאה משינוי כתובות על ידי הממשלה.
    """
    print("--- 🔍 Searching for correct Resource ID... ---")
    
    # כתובת החיפוש הראשי של המאגר הממשלתי
    search_url = "https://data.gov.il/api/3/action/package_search"
    
    # חיפוש לפי מילות מפתח: "עסקאות נדלן"
    params = {
        'q': 'עסקאות נדלן',
        'rows': 5
    }
    
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    
    try:
        response = requests.get(search_url, params=params, headers=headers, timeout=30)
        if response.status_code != 200:
            print(f"❌ Search failed: {response.status_code}")
            return None
            
        results = response.json().get('result', {}).get('results', [])
        
        # מעבר על התוצאות ומציאת המשאב הראשון שהוא CSV
        for package in results:
            for resource in package.get('resources', []):
                # בדיקה שהקובץ הוא CSV ושייך לנושא הנדל"ן
                if resource['format'].upper() == 'CSV' and 'נדל' in resource['name']:
                    print(f"✅ Found Valid Resource: {resource['name']} (ID: {resource['id']})")
                    return resource['id']
                    
        print("⚠️ Could not find an exact match automatically.")
        return None
        
    except Exception as e:
        print(f"❌ Error during ID discovery: {e}")
        return None

def fetch_data():
    # שלב 1: מציאת המזהה הנכון (במקום לנחש אותו)
    resource_id = get_dynamic_resource_id()
    
    if not resource_id:
        # גיבוי: ננסה מזהה נוסף מוכר אם החיפוש נכשל
        print("⚠️ Falling back to default known ID...")
        resource_id = "5fc14c6e-5d12-4293-9799-73e481156e71"

    # שלב 2: שליפת הנתונים עם המזהה שנמצא
    url = f"https://data.gov.il/api/3/action/datastore_search?resource_id={resource_id}&limit=15000"
    
    try:
        print(f"--- 🚀 Starting Fetch from ID: {resource_id} ---")
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
        response = requests.get(url, headers=headers, timeout=60)
        
        if response.status_code == 200:
            data = response.json()
            records = data.get('result', {}).get('records', [])
            
            if records:
                df = pd.DataFrame(records)
                
                # סינון: רק עסקאות שקשורות לתל אביב-יפו
                # שימוש בחיפוש רחב כדי לא לפספס (תל אביב, ת"א וכו')
                df_tlv = df[df['שם יישוב'].astype(str).str.contains('תל אביב', na=False)].copy()
                
                if not df_tlv.empty:
                    filename = "tlv_deals_master.csv"
                    df_tlv.to_csv(filename, index=False, encoding='utf-8-sig')
                    print(f"✅ SUCCESS! Saved {len(df_tlv)} real deals to {filename}")
                else:
                    print(f"⚠️ Data fetched, but no Tel Aviv deals in this specific batch (Total records: {len(df)}).")
                    # שומרים את מה שיש בכל זאת כדי לראות שהקובץ נוצר
                    df.head(100).to_csv("debug_data.csv", index=False, encoding='utf-8-sig')
            else:
                print("❌ API returned empty records list.")
        else:
            print(f"❌ Fetch Error: {response.status_code}")
            sys.exit(1)
            
    except Exception as e:
        print(f"❌ Critical Failure: {e}")
        sys.exit(1)

if __name__ == "__main__":
    fetch_data()
