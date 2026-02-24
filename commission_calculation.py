import pandas as pd
import os
import sys
import time
import requests
from sqlalchemy import create_engine
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from tqdm import tqdm
import xlsxwriter
from concurrent.futures import ThreadPoolExecutor, as_completed
import random
import json
from pathlib import Path
from ratelimit import limits, sleep_and_retry
import certifi


# -----------------------------
# 1. Database Connection Settings
# -----------------------------
USER_MAIN = 'vaportal'
PW_MAIN   = 'vPI7P0G1zV6iERqM28'
HOST_MAIN = 'lisportalprod2.mysql.database.azure.com'
PORT_MAIN = 3306

USER_AUX = 'slave60'
PW_AUX   = 'Vibrant1'
HOST_AUX = '192.168.60.2'
PORT_AUX = 3307

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

engine_main = create_engine(
    f"mysql+pymysql://{USER_MAIN}:{PW_MAIN}@{HOST_MAIN}:{PORT_MAIN}/lis_billing",
    connect_args={"ssl": {
            "ca": certifi.where()  
        }}
)
engine_aux = create_engine(
    f"mysql+pymysql://{USER_AUX}:{PW_AUX}@{HOST_AUX}:{PORT_AUX}/vibrant_statistics"
)

# -----------------------------
# 2. Commission Time range & Currency conversion
# -----------------------------
START_DATE = '2026-01-01'
END_DATE   = '2026-02-01'

# Use end-of-month exchange rate for all currency conversions

# RATES_2025_05_31
# RATES = {
#     'usd':1.00,'gbp':1.346,'eur':1.135,'chf':1 / 0.8227,
#     'nok':1 / 10.218,'dkk':1 / 6.5561,'sek':0.10228,'mxn':1 / 19.430,'jpy':0.00694
# }

# RATES_2025_06_30
# RATES = {
#     'usd':1.00,'gbp':1.355,'eur':1.179,'chf':1.261,
#     'nok':0.09932,'dkk':0.15799,'sek':0.10577,'mxn':0.05333,'jpy':0.00695
# }

# RATES_2025_07_31
# RATES = {
#     'usd': 1.00,
#     'eur': 1.1416,
#     'gbp': 1.3206,
#     'chf': 1.2310,
#     'nok': 0.0969,
#     'sek': 0.1021,
#     'mxn': 0.0530,
#     'jpy': 0.0066,
#     'dkk':0.15799
# }

# RATES_2025_08_31
# RATES = {
#     'usd': 1.0000,
#     'eur': 1.1684,
#     'gbp': 1.3506,
#     'chf': 1.2495,
#     'nok': 0.0994,
#     'sek': 0.1057,
#     'mxn': 0.0536,
#     'jpy': 0.0068,
#     'dkk': 0.1566
# }

# RATES_2025_09_30
# RATES = {
#     'usd': 1.0000,
#     'eur': 1.1684,
#     'gbp': 1.3506,
#     'chf': 1.2495,
#     'nok': 0.0994,
#     'sek': 0.1057,
#     'mxn': 0.0536,
#     'jpy': 0.0068,
#     'dkk': 0.1566
# }

# RATES_2025_10_31
# RATES = {
#     'usd': 1.0000,
#     'eur': 1.1607,   # 1 EUR = 1 / 0.8616 USD
#     'gbp': 1.3150,   # 1 / 0.7602
#     'chf': 1.2429,   # 1 / 0.8046
#     'nok': 0.0989,   # 1 / 10.1162
#     'sek': 0.1054,   # 1 / 9.4922
#     'mxn': 0.0539,   # 1 / 18.5504
#     'jpy': 0.0065,   # 1 / 153.8913
#     'dkk': 0.1546    # 1 / 6.4686
# }

# RATES_2025_11_30
# RATES = {
#     'usd': 1.0000,
#     'eur': 1.1601,   # 1 EUR ≈ 1.1601 USD
#     'gbp': 1.3239,   # 1 GBP ≈ 1.3239 USD
#     'chf': 1.2445,   # 1 CHF ≈ 1.2445 USD
#     'nok': 0.0984,   # 1 NOK ≈ 0.0984 USD
#     'sek': 0.1058,   # 1 SEK ≈ 0.1058 USD
#     'mxn': 0.0547,   # 1 MXN ≈ 0.0547 USD
#     'jpy': 0.00641,  # 1 JPY ≈ 0.00641 USD
#     'dkk': 0.1547    # 1 DKK ≈ 0.1547 USD
# }

# RATES_2025_12_31
# RATES = {
#     'usd': 1.0000,
#     'eur': 1.1733,   # 1 EUR ≈ 1.1733 USD 
#     'gbp': 1.3448,   # 1 GBP ≈ 1.3448 USD 
#     'chf': 1.2590,   # 1 CHF ≈ ~1.2590 USD
#     'nok': 0.0985,   # 1 NOK ≈ ~0.0985 USD
#     'sek': 0.1076,   # 1 SEK ≈ ~0.1076 USD
#     'mxn': 0.0555,   # 1 MXN ≈ ~0.0555 USD
#     'jpy': 0.00638,  # 1 JPY ≈ ~0.00638 USD
#     'dkk': 0.1558    # 1 DKK ≈ ~0.1558 USD
# }

# RATES_2026_01_31 (approximate market FX)
RATES = {
    'usd': 1.0000,
    'eur': 1.20,    # 1 EUR ≈ ~1.20 USD
    'gbp': 1.38,    # 1 GBP ≈ ~1.38 USD
    'chf': 1.31,    # 1 CHF ≈ ~1.31 USD
    'nok': 0.104,   # 1 NOK ≈ ~0.104 USD
    'sek': 0.114,   # 1 SEK ≈ ~0.114 USD
    'mxn': 0.058,   # 1 MXN ≈ ~0.058 USD
    'jpy': 0.0066,  # 1 JPY ≈ ~0.0066 USD
    'dkk': 0.16     # 1 DKK ≈ ~0.16 USD
}

# -----------------------------
# 3. Read Table lis_billing.applies data and split positive/negative revenue
# -----------------------------
query_applies = f"""
SELECT a.id AS apply_id,
       a.charge_id,
       a.apply_amount,
       LOWER(c.currency) AS currency,
       c.charge_type_id AS order_id,
       a.created_at
FROM lis_billing.applies a
LEFT JOIN lis_billing.charges c ON a.charge_id = c.id
WHERE a.deleted_at IS NULL
  AND a.created_at >= '{START_DATE}'
  AND a.created_at <  '{END_DATE}';
"""
df_applies = pd.read_sql(query_applies, engine_main)
if df_applies.empty:
    print("No applies data found for the specified date range.")
    sys.exit(0)

df_applies['revenue_usd'] = df_applies['apply_amount'] * df_applies['currency'].map(RATES)
df_applies['revenue_pos'] = df_applies['revenue_usd'].clip(lower=0)
df_applies['revenue_neg'] = df_applies['revenue_usd'].clip(upper=0)

df_revenue = (
    df_applies
      .groupby('order_id', as_index=False)
      .agg({'revenue_pos':'sum','revenue_neg':'sum'})
)
order_ids = df_revenue['order_id'].dropna().astype(int).unique().tolist()
if not order_ids:
    print("No valid order_id found.")
    sys.exit(0)

# -----------------------------
# 4. Fetch orders from lis_re.order_table, and join sales_user_id
# -----------------------------
def chunked_list(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

detail_frames = []
for chunk in chunked_list(order_ids, 1000):
    ids_sql = ",".join(map(str,chunk))
    # --- Coresample V1 to V2
    # q = f"""
    # SELECT o.id           AS order_id,
    #        o.sample_id    AS sample_id,
    #        o.customer_id  AS customer_id,
    #        CONCAT(cu.customer_first_name,' ',cu.customer_last_name) AS customer_name,
    #        o.patient_id   AS patient_id,
    #        o.created_date AS created_date,
    #        o.charge_method,
    #        o1.sales_id    AS sales_user_id
    # FROM lis_re.order_table o
    # LEFT JOIN lis_core_v7.customer cu 
    #   ON o.customer_id = cu.customer_id
    # LEFT JOIN lis_core_v7.order_info o1 
    #   ON o1.billing_order_id = CONCAT(o.id, '')
    # WHERE o.id IN ({ids_sql})
    #   AND o.charge_method <> 'wellProz'
    #   AND o.sample_id not in (2322399, 2322403, 2322409, 2322976, 2322406, 2322412, 2346134);
    # """
    q = f"""
    SELECT o.id           AS order_id,
           o.sample_id    AS sample_id,
           o.customer_id  AS customer_id,
           CONCAT(cu.customer_first_name,' ',cu.customer_last_name) AS customer_name,
           o.patient_id   AS patient_id,
           o.created_date AS created_date,
           o.charge_method,
           o1.sales_id    AS sales_user_id
    FROM lis_re.order_table o
    LEFT JOIN coresamplesv2.customer cu 
      ON o.customer_id = cu.customer_id
    LEFT JOIN coresamplesv2.order_info o1 
      ON o1.billing_order_id = CONCAT(o.id, '')
    WHERE o.id IN ({ids_sql})
      AND o.charge_method <> 'wellProz'
      AND o.sample_id not in (2322399, 2322403, 2322409, 2322976, 2322406, 2322412, 2346134);
    """
    detail_frames.append(pd.read_sql(q, engine_main))

df_orders = pd.concat(detail_frames, ignore_index=True)
df_orders['order_id'] = df_orders['order_id'].astype(int)

df_with_sample = df_orders[df_orders['sample_id'].notna()].copy()
df_with_sample['sample_id'] = df_with_sample['sample_id'].astype(int)
missing = []
for chunk in chunked_list(df_with_sample['sample_id'].unique().tolist(), 1000):
    exist = pd.read_sql(
        f"SELECT sample_id FROM lis_core_v7.sample WHERE sample_id IN ({','.join(map(str,chunk))});",
        engine_main
    )['sample_id'].astype(int).tolist()
    missing += [sid for sid in chunk if sid not in exist]
if missing:
    print("Missing sample_id values:", missing[:100])
    sys.exit(1)

# -----------------------------
# 5. Fetch Well Proz orders and Call Kang's API for revenue
# -----------------------------

# 5.1 Get Well Proz orders through SQL

# --- Coresample V1 to V2
# query = f"""
# SELECT 
#     o.id                             AS order_id,
#     o.sample_id,
#     o.customer_id,
#     CONCAT(cu.customer_first_name, ' ', cu.customer_last_name) AS customer_name,
#     o.patient_id,
#     o.created_date,
#     o.julien_barcode,
#     o1.sales_id                      AS sales_user_id,
#     iu.internal_user_role_id         AS sales_role_id
# FROM lis_re.order_table o
# LEFT JOIN lis_core_v7.customer cu 
#     ON o.customer_id = cu.customer_id
# LEFT JOIN lis_core_v7.order_info o1
#     ON CAST(TRIM(o1.billing_order_id) AS UNSIGNED) = o.id
# LEFT JOIN lis_core_v7.internal_user iu
#     ON iu.internal_user_id = o1.sales_id 
#    AND iu.internal_user_role = 'sales'
# WHERE o.charge_method = 'wellProz'
#   AND o.created_date >= '{START_DATE}'
#   AND o.created_date < '{END_DATE}'
#   AND COALESCE(o.note, '') <> 'redraw'
#   AND o.sample_id not in (2316492, 2326004, 2340768, 2356728, 2391972,2460864)
# ;
# """

query = f"""
SELECT 
    o.id                             AS order_id,
    o.sample_id,
    o.customer_id,
    CONCAT(cu.customer_first_name, ' ', cu.customer_last_name) AS customer_name,
    o.patient_id,
    o.created_date,
    o.julien_barcode,
    o1.sales_id                      AS sales_user_id,
    iu.internal_user_role_id         AS sales_role_id
FROM lis_re.order_table o
LEFT JOIN coresamplesv2.customer cu 
    ON o.customer_id = cu.customer_id
LEFT JOIN coresamplesv2.order_info o1
    ON CAST(TRIM(o1.billing_order_id) AS UNSIGNED) = o.id
LEFT JOIN coresamplesv2.internal_user iu
    ON iu.internal_user_id = o1.sales_id 
   AND iu.internal_user_role = 'sales'
WHERE o.charge_method = 'wellProz'
  AND o.created_date >= '{START_DATE}'
  AND o.created_date < '{END_DATE}'
  AND COALESCE(o.note, '') <> 'redraw'
  AND o.sample_id not in (2316492, 2326004, 2340768, 2356728, 2391972,2460864,2238778,
2234594,
2234588,
2227061,
2227077,
2238784,
2233580,
2238781,
2227323,
2224966,
2238786,
2238777,
2238779,
2225374,
2227325)
;
"""
df_wellProz = pd.read_sql(query, engine_main)
if df_wellProz.empty:
    print("No WellProz orders found. ")
    sys.exit(0)

# 5.2 Handle Exception
missing = df_wellProz[df_wellProz['sales_user_id'].isna()]['order_id'].tolist()
print("The following orders have no sales_id in order_info:", missing)
df_wellProz['sales_user_id'] = df_wellProz['sales_user_id'].fillna(-1).astype(int)

# 5.3 Local Cache
CACHE_FILE = Path(BASE_DIR) / "profit_cache.json"
if CACHE_FILE.exists():
    profit_map = json.loads(CACHE_FILE.read_text())
else:
    profit_map = {}

# 5.4 Session
session = requests.Session()
session.mount("https://", HTTPAdapter(max_retries=Retry(total=0)))

# 5.5 Add ratelimit
ONE_SECOND = 1
@sleep_and_retry
@limits(calls=1, period=ONE_SECOND)
def fetch_profit_once(barcode):
    url = f"https://api.wellproz.com/wellproz_api/order/seller/getLabProfitByBarcode?barcode={barcode}"
    resp = session.get(url, timeout=10)
    resp.raise_for_status()
    txt = resp.text
    return float(txt[txt.find(':')+1:txt.rfind('}')].strip())

# 5.6 Wrap it with exponential backoff:
def fetch_with_backoff(barcode, max_attempts = 5):
    if not barcode:
        return 0.0
    if barcode in profit_map:
        return profit_map[barcode]

    delay = 1
    for attempt in range(1, max_attempts + 1):
        try:
            val = fetch_profit_once(barcode)
            profit_map[barcode] = val
            return val
        except requests.exceptions.HTTPError as e:
            code = e.response.status_code
            if code == 429:
                # Read Retry-After when 429
                ra = e.response.headers.get("Retry-After")
                wait = float(ra) if ra and ra.isdigit() else delay
                print(f"[WARN ] {barcode}: 429, retry after {wait}s")
                time.sleep(wait + random.uniform(0,0.5))
            elif 500 <= code < 600:
                print(f"[WARN ] {barcode}: {code} server error, retry in {delay}s")
                time.sleep(delay + random.uniform(0,0.5))
            else:
                print(f"[ERROR] {barcode}: HTTP {code}, give up")
                break
            delay *= 2
        except Exception as e:
            print(f"[ERROR] {barcode}: {e}, give up")
            break

    profit_map[barcode] = 0.0
    return 0.0

# 5.7 Parallel fetch for uncached barcodes
codes = df_wellProz['julien_barcode'].dropna().unique().tolist()
to_fetch = [bc for bc in codes if bc not in profit_map]

with ThreadPoolExecutor(max_workers=2) as executor:
    results = executor.map(fetch_with_backoff, to_fetch)
    for bc, val in tqdm(
        zip(to_fetch, results),
        total=len(to_fetch),
        desc="Fetching profit"
    ):
        profit_map[bc] = val

# 5.8 Update cache
CACHE_FILE.write_text(json.dumps(profit_map))

# 5.9 Map results back onto the DataFrame:
df_wellProz['revenue_usd']     = df_wellProz['julien_barcode'].map(profit_map).fillna(0.0)
df_wellProz['revenue_payment'] = df_wellProz['revenue_usd'].clip(lower=0)
df_wellProz['revenue_refund']  = df_wellProz['revenue_usd'].clip(upper=0)

# -----------------------------
# 6. Merge revenue & filter entries without sales
# -----------------------------
df_orders['order_id']  = pd.to_numeric(df_orders['order_id'], errors='raise').astype(int)
df_revenue['order_id'] = pd.to_numeric(df_revenue['order_id'], errors='raise').astype(int)

df_details = (
    df_orders
    .merge(df_revenue, on='order_id', how='inner')
)
df_details = df_details[df_details['sales_user_id'].notna()]
df_details['sales_user_id'] = df_details['sales_user_id'].astype(int)
df_details['customer_name'] = df_details['customer_name'].fillna('Direct Order')

# -----------------------------
# 7. Fetch the sales mapping
# -----------------------------
sales_ids = df_details['sales_user_id'].unique().tolist()
if sales_ids:
    df_int = pd.read_sql(
        f"""
        SELECT internal_user_id AS id,
               internal_user_name,
               internal_user_role_id
        # -- FROM lis_core_v7.internal_user
        FROM coresamplesv2.internal_user
        WHERE internal_user_id IN ({','.join(map(str,sales_ids))})
          AND internal_user_role='sales';
        """,
        engine_main
    )
else:
    df_int = pd.DataFrame(columns=['id','internal_user_name','internal_user_role_id'])

df_details = df_details.merge(
    df_int[['id','internal_user_role_id']],
    left_on='sales_user_id', right_on='id', how='left'
).rename(columns={'internal_user_role_id':'sales_role_id'}).drop(columns=['id'])

# ===== DEBUG: find missing sales_role_id =====
bad = df_details[df_details['sales_role_id'].isna()][
    ['order_id','sales_user_id','customer_id','created_date']
].head(50)

print("Rows with missing sales_role_id (showing up to 50):")
print(bad)

print("Missing sales_role_id count:", df_details['sales_role_id'].isna().sum())
print(
    "Distinct sales_user_id missing role:",
    df_details.loc[df_details['sales_role_id'].isna(), 'sales_user_id'].nunique()
)
print(
    "Example missing sales_user_ids:",
    df_details.loc[df_details['sales_role_id'].isna(), 'sales_user_id']
      .dropna().astype(int).unique()[:50]
)
# ===== END DEBUG =====

# -----------------------------
# 8. Fetch Commission Rule from Table vibrant_statistics.comm_sales_customer_distribute_new
# -----------------------------
df_dist = pd.read_sql(
    "SELECT sales_id,customer_id,ratio,to_sales FROM vibrant_statistics.comm_sales_customer_distribute_new;",
    engine_aux
)
general_map, specific_map = {}, {}
for _,r in df_dist.iterrows():
    sid   = int(r['sales_id'])
    cid   = r['customer_id']
    ratio = float(r['ratio'])
    to    = int(r['to_sales'])
    if pd.isna(cid):
        general_map.setdefault(sid,[]).append((ratio,to))
    else:
        specific_map.setdefault((sid,int(cid)),[]).append((ratio,to))

# -----------------------------
# 9. Handle Exceptions
# -----------------------------
order_ids2 = df_details['order_id'].unique().tolist()
df_od = pd.read_sql(
    f"SELECT order_id,item_type_id,markup FROM lis_re.order_detail WHERE order_id IN ({','.join(map(str,order_ids2))});",
    engine_main
)
od_map = df_od.groupby('order_id')['item_type_id'].apply(list).to_dict()

# 9.1 Skincare & NutriProz revenue
def adjust_rev(r):
    total = r['revenue_pos'] + r['revenue_neg']
    if total <= 0:
        return r['revenue_pos'], r['revenue_neg']
    types = od_map.get(r['order_id'],[])
    if 30001 in types:  # NutriproZ
        return r['revenue_pos']-1051, r['revenue_neg']
    if 30003 in types:  # Skincare
        return r['revenue_pos']-101, r['revenue_neg']
    if any(30000<it<31000 and it not in (30001,30003) for it in types):
        return 0.0, 0.0
    return r['revenue_pos'], r['revenue_neg']

df_details[['revenue_pos','revenue_neg']] = df_details.apply(
    lambda r: pd.Series(adjust_rev(r)),axis=1
)

# 9.2 Fetch and deduct markup
mark_map = df_od.groupby('order_id')['markup'].sum().fillna(0).to_dict()
df_details['total_markup'] = df_details['order_id'].map(mark_map).fillna(0)
mask = (df_details['revenue_pos'] + df_details['revenue_neg']) > 0
df_details.loc[mask, 'revenue_pos'] -= df_details.loc[mask, 'total_markup']

# 9.3 Fetch consultation fee
df_cf = pd.read_sql(
    f"""
    SELECT order_id, CONVERT(value USING utf8) AS fee_value
    FROM lis_re.order_additional_info
    WHERE `key` = 'consultationFee'
      AND order_id IN ({','.join(map(str,order_ids2))});
    """,
    engine_main
)
df_cf['consultation_fee'] = df_cf['fee_value'].map(lambda x: float(x.decode('utf-8')) if isinstance(x, (bytes,bytearray)) else float(x)).fillna(0)
consult_map = df_cf.set_index('order_id')['consultation_fee'].to_dict()
df_details['consultation_fee'] = df_details['order_id'].map(consult_map).fillna(0)
df_details.loc[mask, 'revenue_pos'] -= df_details.loc[mask, 'consultation_fee']

# 9.4 Fetch concierge fee
df_cq = pd.read_sql(
    f"SELECT order_id, total AS concierge_fee FROM lis_re.concierge_fee WHERE order_id IN ({','.join(map(str,order_ids2))});",
    engine_main
)
df_cq['concierge_fee'] = df_cq['concierge_fee'].astype(float)
cq_map = df_cq.set_index('order_id')['concierge_fee'].to_dict()
df_details['concierge_fee'] = df_details['order_id'].map(cq_map).fillna(0)
df_details.loc[mask, 'revenue_pos'] -= df_details.loc[mask, 'concierge_fee']

# 9.5 Compute special deduction labels for NutriProZ/Skincare
def special_amount_and_label(r):
    types = od_map.get(r['order_id'], [])
    if 30001 in types:
        return 1051.0, "NutriProZ"
    if 30003 in types:
        return 101.0, "Skincare"
    return 0.0, ""
df_details[['special_deduction','special_label']] = df_details.apply(
    lambda r: pd.Series(special_amount_and_label(r)),
    axis=1
)

# 9.6 Build the comment column
def format_comment(r):
    parts = []
    # NutriProZ/Skincare
    if r['special_deduction'] > 0:
        parts.append(f"${r['special_deduction']:.2f} {r['special_label']}")
    # Markup
    if r['total_markup'] > 0:
        parts.append(f"${r['total_markup']:.2f} Markup")
    # Consultation Fee
    if r['consultation_fee'] > 0:
        parts.append(f"${r['consultation_fee']:.2f} Consultation Fee")
    # Concierge Fee
    if r['concierge_fee'] > 0:
        parts.append(f"${r['concierge_fee']:.2f} Concierge Fee")
    return "; ".join(parts)

df_details['comment'] = df_details.apply(format_comment, axis=1)

# 9.7 For each sample_id with multiple order_ids, keep only the smallest order_id
# (e.g., a redraw creates a new order_id for the same sample_id)
df_details = (
    df_details
    .sort_values('order_id')
    .drop_duplicates(subset=['sample_id'], keep='first')
)

# -----------------------------
# 10. Commission Calculation 
# -----------------------------
def calc_comm(r):
    role_id = int(r['sales_role_id'])
    base    = (r['revenue_pos'] + r['revenue_neg']) * 0.04
    cust_id = int(r['customer_id'])

    if (role_id, cust_id) in specific_map:
        rules = specific_map[(role_id, cust_id)]
    else:
        rules = general_map.get(role_id, [])

    total_paid = 0.0
    paid_to_senior = 0.0
    senior   = None
    
    for ratio, to_sales in rules:
        share = base * (ratio / 100)
        total_paid += share
        if to_sales and to_sales != 0:
            paid_to_senior += share
            senior = to_sales

    return pd.Series([paid_to_senior, senior, total_paid],
                     index=['commission_paid_to_senior','senior_sales_id','commission_paid_total']
    )

df_details[['commission_paid_to_senior','senior_sales_id','commission_paid_total']] = \
    df_details.apply(calc_comm,axis=1)
df_details['commission_from_revenue'] = (df_details['revenue_pos'] + df_details['revenue_neg']) * 0.04
df_details['commission_final'] = df_details['commission_from_revenue'] - df_details['commission_paid_total']
df_details['order_time'] = pd.to_datetime(df_details['created_date']).dt.strftime('%m/%d/%Y')

# Define Senior Relationship for Excel Generation
def resolve_senior(sid):
    if sid in (0, 7, 13, 31):
        return sid
    # Look up general_map for redistribution rules and select the first non-zero to_sales value, if any
    for ratio, to in general_map.get(sid, []):
        if to and to != 0:
            return to
    return 0

# -----------------------------
# 11. Generate Commission Allocation Details and Apply Fixed Column Widths
# -----------------------------
def write_details(df, out_path):
    df = df.copy()
    df['order_time'] = pd.to_datetime(df['created_date']).dt.strftime('%m/%d/%Y')

    df_out = (
        df
        .groupby(
            ['sample_id','patient_id','customer_id','customer_name','order_time'],
            as_index=False
        )
        .agg({
            'sales_role_id':           'first',
            'revenue_pos':             'sum',
            'revenue_neg':             'sum',
            'commission_from_revenue': 'sum',
            'commission_final':        'sum',
            'comment':                 'first'
        })
        .rename(columns={
            'sales_role_id': 'sales_id',
            'revenue_pos':   'revenue_payment',
            'revenue_neg':   'revenue_refund'
        })
    )

    cols = [
        'sample_id','patient_id','customer_id','customer_name',
        'order_time','sales_id','revenue_payment','revenue_refund',
        'commission_from_revenue','commission_final','comment'
    ]
    df_out = df_out[cols]

    with pd.ExcelWriter(out_path, engine='xlsxwriter') as w:
        df_out.to_excel(w, sheet_name='Details', index=False)
        ws = w.sheets['Details']
        col_widths = {
            'A':12,'B':12,'C':12,'D':12,'E':20,
            'F':12,'G':15,'H':15,'I':18,'J':16,'K':30
        }
        for col, wd in col_widths.items():
            ws.set_column(f"{col}:{col}", wd)

# -----------------------------
# 12. Generate Commission Summary
# -----------------------------
def write_summary(df, df_int, out_path):
    df = df.copy()
    df['net_revenue'] = df['revenue_pos'] + df['revenue_neg']

    rev_grp = (
        df.groupby('sales_user_id', as_index=False)['net_revenue']
          .sum()
          .rename(columns={'sales_user_id':'new_sales_id','net_revenue':'revenue'})
    )
    paid_out = (
        df.groupby('sales_user_id', as_index=False)['commission_paid_total']
          .sum()
          .rename(columns={'sales_user_id':'new_sales_id','commission_paid_total':'commission_paid_out'})
    )
    paid_out['commission_to_others'] = -paid_out['commission_paid_out']
    paid_in = (
        df.loc[df['senior_sales_id'].notna()]
        .groupby('senior_sales_id', as_index=False)['commission_paid_to_senior']
        .sum()
        .rename(columns={'senior_sales_id':'sales_id','commission_paid_to_senior':'commission_from_others'})
    )

    all_roles = set(df_int['internal_user_role_id'].dropna().astype(int)) | {0}
    df_int_all = df_int[
        df_int['internal_user_role_id'].isin(all_roles)
    ].rename(columns={
        'id':'new_sales_id',
        'internal_user_name':'sales_name',
        'internal_user_role_id':'sales_id'
    })

    summary = (
        df_int_all
        .merge(rev_grp, on='new_sales_id', how='left')
        .fillna({'revenue':0})
    )
    summary['commission_from_self'] = summary['revenue'] * 0.04
    summary = summary.merge(
        paid_out[['new_sales_id','commission_to_others']],
        on='new_sales_id', how='left'
    ).fillna({'commission_to_others':0})
    summary = summary.merge(paid_in, on='sales_id', how='left').fillna({'commission_from_others':0})

    summary['senior_sales_id'] = summary['sales_id'].apply(resolve_senior).astype(int)
    summary['commission_final'] = (
        summary['commission_from_self']
      + summary['commission_from_others']
      + summary['commission_to_others']
    )
    for c in [
        'revenue','commission_from_self',
        'commission_from_others','commission_to_others',
        'commission_final'
    ]:
        summary[c] = summary[c].round(2)

    cols = [
        'new_sales_id','sales_id','senior_sales_id','sales_name',
        'revenue','commission_from_self','commission_from_others',
        'commission_to_others','commission_final'
    ]
    summary = summary[cols]

    with pd.ExcelWriter(out_path, engine='xlsxwriter') as w:
        summary.to_excel(w, sheet_name='Summary', index=False)
        ws2 = w.sheets['Summary']
        widths2 = {
            'A':12,'B':12,'C':14,'D':20,
            'E':15,'F':18,'G':18,'H':18,'I':18
        }
        for col, wd in widths2.items():
            ws2.set_column(f"{col}:{col}", wd)

# -----------------------------
# 13. Generate Details Excel & Summary Excel
# -----------------------------

write_details(df_details, 'details.xlsx')
write_summary(df_details, df_int, 'summary.xlsx')
print("✅ Successfully generated details.xlsx & summary.xlsx")

# Well Proz Excels
df_wp = df_wellProz.rename(columns={
    'revenue_payment':'revenue_pos',
    'revenue_refund':'revenue_neg'
}).copy()

df_wp['comment'] = ''

df_wp[['commission_paid_to_senior','senior_sales_id','commission_paid_total']] = \
    df_wp.apply(calc_comm, axis=1)
df_wp['commission_from_revenue'] = (df_wp['revenue_pos'] + df_wp['revenue_neg']) * 0.04
df_wp['commission_final'] = df_wp['commission_from_revenue'] - df_wp['commission_paid_total']

write_details(df_wp,      'well_proz_details.xlsx')
write_summary(df_wp, df_int, 'well_proz_summary.xlsx')
print("✅ Successfully generated well_proz_details.xlsx & well_proz_summary.xlsx")