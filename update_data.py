import requests
import pandas as pd
import sys
import datetime
import json

# הגדרת קידוד להדפסת עברית תקינה בלוגים
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')

def find_alive_resource():
    """
    פונקציה זו סורקת את המאגר הממשלתי עם מספר מילות מפתח
    ומחזירה את ה-Resource ID הראשון שבאמת עובד ומחזיר נתונים.
    """
    print("--- 🕵️ Starting Intelligent Resource Discovery ---")
    
    # רשימת מילות מפתח לחיפוש רחב כדי לא לפספס את המאגר
    search_terms = ["עסקאות נדלן", "נדלן", "Real Estate", "Transactions", "כרמן"]
    base_search_url = "https://data.gov.il/api/3/action/package_search"
    base_data_url = "https://data.gov.il/api/3/action/datastore_search"
    
    candidates = []

    # שלב 1: איסוף מועמדים
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    
    for term in search_terms:
        try:
            print(f"🔎 Searching for keyword: '{term}'...")
            params = {'q': term, 'rows': 5}
            response = requests.get(base_search_url, params=params, headers=headers, timeout=10)
            
            if response.status_code == 200:
                results = response.json().get('result', {}).get('results', [])
                for pkg in results:
                    for res in pkg.get('resources', []):
                        # סינון גס: אנחנו מחפשים קבצי CSV רלוונטיים
                        if 'CSV' in res.get('format', '').upper():
                            candidates.append({
                                'name': res.get('name', 'Unknown'),
                                'id': res['id'],
                                'pkg_title': pkg.get('title', 'Unknown')
                            })
        except Exception as e:
            print(f"⚠️ Search error for '{term}': {e}")

    # הסרת כפילויות
    unique_candidates = {v['id']: v for v in candidates}.values()
    print(f"📋 Found {len(unique_candidates)} potential resources. Testing connectivity...")

    # שלב 2: בדיקת "דופק" לכל מועמד
    for cand in unique_candidates:
        res_id = cand['id']
        name = cand['name']
        print(f"👉 Testing ID: {res_id} ({name})...", end=" ")
        
        try:
            # מנסים לשלוף רק שורה אחת כדי לראות אם השרת מגיב
            test_url = f"{base_data_url}?resource_id={res_id}&limit=1"
            resp = requests.get(test_url, headers=headers, timeout=15)
            
            if resp.status_code == 200:
                data = resp.json()
                if data.get('success') and data.get('result', {}).get('records'):
                    print("✅ ALIVE! Found valid data.")
                    return res_id
                else:
                    print("❌ Empty response.")
            else:
                print(f"❌ Error {resp.status_code}")
        except Exception as e:
            print(f"❌ Exception: {e}")

    print("💀 All searches failed. No alive resource found.")
    return None

def fetch_data():
    # חיפוש המזהה החי
    resource_id = find_alive_resource()
    
    if not resource_id:
        print("❌ CRITICAL: Could not find any active Real Estate resource ID.")
        # הדפסת כל המועמדים ללוג כדי שתוכל לשלוח לי לניתוח אם זה נכשל
        sys.exit(1)

    # ביצוע השליפה המלאה עם המזהה שנמצא
    url = f"https://data.gov.il/api/3/action/datastore_search?resource_id={resource_id}&limit=32000"
    
    try:
        print(f"--- 🚀 Starting Full Fetch from Verified ID: {resource_id} ---")
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
        response = requests.get(url, headers=headers, timeout=120)
        
        if response.status_code == 200:
            records = response.json().get('result', {}).get('records', [])
            if records:
                df = pd.DataFrame(records)
                
                # הדפסת עמודות ללוג לצורך דיבאגינג
                print(f"Columns found: {list(df.columns)}")
                
                # סינון חכם: מציאת עמודת היישוב באופן דינמי (למקרה שהשם השתנה)
                city_col = next((col for col in df.columns if 'יישוב' in col or 'city' in col.lower()), None)
                
                if city_col:
                    df_tlv = df[df[city_col].astype(str).str.contains('תל אביב', na=False)].copy()
                    filename = "tlv_deals_master.csv"
                    df_tlv.to_csv(filename, index=False, encoding='utf-8-sig')
                    print(f"✅ SUCCESS! Saved {len(df_tlv)} Tel Aviv deals to {filename}")
                else:
                    print("⚠️ Could not identify 'City' column. Saving raw file...")
                    df.to_csv("raw_data_debug.csv", index=False, encoding='utf-8-sig')
            else:
                print("❌ API returned 0 records.")
        else:
            print(f"❌ Full fetch failed: {response.status_code}")
            sys.exit(1)
            
    except Exception as e:
        print(f"❌ Critical Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    fetch_data()
