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

CA_FILE = os.path.join(BASE_DIR, "DigiCertGlobalRootCA.crt.pem")
if not os.path.isfile(CA_FILE):
    raise FileNotFoundError(f"Cannot find SSL file: {CA_FILE}")

engine_main = create_engine(
    f"mysql+pymysql://{USER_MAIN}:{PW_MAIN}@{HOST_MAIN}:{PORT_MAIN}/lis_billing",
    connect_args={"ssl":{"ca": CA_FILE}}
)
engine_aux = create_engine(
    f"mysql+pymysql://{USER_AUX}:{PW_AUX}@{HOST_AUX}:{PORT_AUX}/vibrant_statistics"
)

# -----------------------------
# 2. Commission Time range & Currency conversion
# -----------------------------
START_DATE = '2025-04-01'
END_DATE   = '2025-05-01'

# Use end-of-month exchange rate for all currency conversions
RATES = {
    'usd':1.00,'gbp':1.335,'eur':1.085,'chf':0.96541,
    'nok':0.09609,'dkk':0.15210,'sek':0.10228,'mxn':0.05120,'jpy':0.006902
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
    LEFT JOIN lis_core_v7.order_info o1 
      ON o1.billing_order_id = CONCAT(o.id, '')
    WHERE o.id IN ({ids_sql})
      AND o.charge_method <> 'wellProz';
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
LEFT JOIN lis_core_v7.order_info o1
    ON CAST(TRIM(o1.billing_order_id) AS UNSIGNED) = o.id
LEFT JOIN lis_core_v7.internal_user iu
    ON iu.internal_user_id = o1.sales_id 
   AND iu.internal_user_role = 'sales'
WHERE o.charge_method = 'wellProz'
  AND o.created_date >= '{START_DATE}'
  AND o.created_date < '{END_DATE}'
  AND COALESCE(o.note, '') <> 'redraw'
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
        FROM lis_core_v7.internal_user
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

# 9.1 Skincare & NutriProz revenue
od_map = df_od.groupby('order_id')['item_type_id'].apply(list).to_dict()
def adjust_rev(r):
    total = r['revenue_pos'] + r['revenue_neg']
    if total <= 0:
        return r['revenue_pos'], r['revenue_neg']
    types = od_map.get(r['order_id'],[])
    if 30001 in types:
        return r['revenue_pos']-1051, r['revenue_neg']
    if 30003 in types:
        return r['revenue_pos']-101, r['revenue_neg']
    if any(30000<it<31000 and it not in (30001,30003) for it in types):
        return 0.0, 0.0
    return r['revenue_pos'], r['revenue_neg']

df_details[['revenue_pos','revenue_neg']] = df_details.apply(
    lambda r: pd.Series(adjust_rev(r)),axis=1
)

# 9.2 Add Comment
comment_map = {}
for oid,ids in od_map.items():
    if   30001 in ids: comment_map[oid] = "NutriProZ"
    elif 30003 in ids: comment_map[oid] = "Skincare"
    elif any(30000<i<31000 and i not in (30001,30003) for i in ids):
        comment_map[oid] = "No tests purchased"
    else:
        comment_map[oid] = ""
df_details['comment'] = df_details['order_id'].map(comment_map).fillna("")

# 9.3 Handle markup
mark_map = df_od.groupby('order_id')['markup'] \
               .sum() \
               .fillna(0) \
               .to_dict()
df_details['total_markup'] = df_details['order_id'].map(mark_map).fillna(0)
# Only deduct markup when revenue_pos + revenue_neg > 0, since commissions before April 2025 did not include markup
mask = (df_details['revenue_pos'] + df_details['revenue_neg']) > 0
df_details.loc[mask, 'revenue_pos'] -= df_details.loc[mask, 'total_markup']

# 9.4 Add "Markup" in Comment field
df_details['comment'] = df_details.apply(
    lambda r: (r['comment']+";Markup") if r['total_markup']>0 and r['comment']!="" 
                else ("Markup" if r['total_markup']>0 else r['comment']),
    axis=1
)

# 9.5 Fetch Consultation Fee and add Comment
df_cf = pd.read_sql(
    f"""
    SELECT order_id,
           CONVERT(value USING utf8) AS fee_value
    FROM lis_re.order_additional_info
    WHERE `key` = 'consultationFee'
      AND order_id IN ({','.join(map(str, order_ids2))});
    """,
    engine_main
)

def parse_fee(x):
    if pd.isna(x):
        return 0.0
    if isinstance(x, (bytes, bytearray)):
        x = x.decode('utf-8')
    return float(x)

df_cf['consultation_fee'] = df_cf['fee_value'].apply(parse_fee)

consultation_map = df_cf.set_index('order_id')['consultation_fee'].to_dict()
df_details['consultation_fee'] = df_details['order_id'].map(consultation_map).fillna(0.0)

df_details.loc[mask, 'revenue_pos'] -= df_details.loc[mask, 'consultation_fee']

df_details['comment'] = df_details.apply(
    lambda r: (r['comment'] + '; Consultation Fee') 
              if r['consultation_fee'] > 0 and r['comment'] != ''
              else ('Consultation Fee' if r['consultation_fee'] > 0 else r['comment']),
    axis=1
)

# 9.6 Fetch Concierge Fee and add Comment
df_cq = pd.read_sql(
    f"""
    SELECT order_id,
           total AS concierge_fee
    FROM lis_re.concierge_fee
    WHERE order_id IN ({','.join(map(str, order_ids2))});
    """,
    engine_main
)

df_cq['concierge_fee'] = df_cq['concierge_fee'].astype(float)
concierge_map = df_cq.set_index('order_id')['concierge_fee'].to_dict()
df_details['concierge_fee'] = df_details['order_id'].map(concierge_map).fillna(0.0)

df_details.loc[mask, 'revenue_pos'] -= df_details.loc[mask, 'concierge_fee']

df_details['comment'] = df_details.apply(
    lambda r: (r['comment'] + '; Concierge Fee')
              if r['concierge_fee'] > 0 and r['comment'] != ''
              else ('Concierge Fee' if r['concierge_fee'] > 0 else r['comment']),
    axis=1
)

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