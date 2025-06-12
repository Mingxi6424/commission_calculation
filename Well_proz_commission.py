import os
import sys
import time
import requests
import pandas as pd
from sqlalchemy import create_engine
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from tqdm import tqdm

# -----------------------------
# 1. DB 连接：默认库改成 lis_re
# -----------------------------
USER_MAIN = 'vaportal'
PW_MAIN = 'vPI7P0G1zV6iERqM28'
HOST_MAIN = 'lisportalprod2.mysql.database.azure.com'
PORT_MAIN = 3306

USER_AUX = 'slave60'
PW_AUX = 'Vibrant1'
HOST_AUX = '192.168.60.2'
PORT_AUX = 3307

engine_main = create_engine(
    f"mysql+pymysql://{USER_MAIN}:{PW_MAIN}@{HOST_MAIN}:{PORT_MAIN}/lis_re",
    connect_args={"ssl": {"ca": os.path.join(os.path.dirname(__file__), "DigiCertGlobalRootCA.crt.pem")}}
)
engine_aux = create_engine(
    f"mysql+pymysql://{USER_AUX}:{PW_AUX}@{HOST_AUX}:{PORT_AUX}/vibrant_statistics"
)

# -----------------------------
# 2. 时间窗口
# -----------------------------
START_DATE = '2025-04-01'
END_DATE = '2025-05-01'

# -----------------------------
# 3. 一次性拉表 + join（注意 CAST+TRIM + 忽略 redraw）
# -----------------------------
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
FROM order_table o
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
df = pd.read_sql(query, engine_main)
if df.empty:
    print("没有符合条件的 wellProz 订单，程序退出。")
    sys.exit(0)

# -----------------------------
# 4. 检查漏匹配
# -----------------------------
missing = df[df['sales_user_id'].isna()]['order_id'].tolist()
print("下列订单在 order_info 里拿不到 sales_id：", missing)
df['sales_user_id'] = df['sales_user_id'].fillna(-1).astype(int)

# -----------------------------
# 5. 配置 session + 重试
# -----------------------------
session = requests.Session()
retries = Retry(
    total=5,
    backoff_factor=0.5,
    status_forcelist=[500, 502, 503, 504],
    raise_on_status=False
)
session.mount("https://", HTTPAdapter(max_retries=retries))

def fetch_profit(barcode):
    if not barcode:
        return 0.0
    url = f"https://api.wellproz.com/wellproz_api/order/seller/getLabProfitByBarcode?barcode={barcode}"
    try:
        r = session.get(url, timeout=10)
        r.raise_for_status()
        txt = r.text
        return float(txt[txt.find(':') + 1: txt.rfind('}')].strip())
    except Exception as err:
        print(f"[ERROR] {barcode}: {err}")
        return 0.0

# -----------------------------
# 6. 顺序调用 API 拿利润，避开限流
# -----------------------------
profits = []
for barcode in tqdm(df['julien_barcode'], desc="Fetching profit"):
    profits.append(fetch_profit(barcode))
    time.sleep(0.3)  # 根据限流情况调整

df['revenue_usd']     = profits
df['revenue_payment'] = df['revenue_usd'].clip(lower=0)
df['revenue_refund']  = df['revenue_usd'].clip(upper=0)

# -----------------------------
# 7. 读取分成规则
# -----------------------------
df_dist = pd.read_sql(
    "SELECT sales_id, customer_id, ratio, to_sales FROM comm_sales_customer_distribute_new;",
    engine_aux
)
general_map, specific_map = {}, {}
for _, row in df_dist.iterrows():
    sid = int(row['sales_id'])
    cid = row['customer_id']
    ratio = float(row['ratio'])
    to = int(row['to_sales'])
    if pd.isna(cid):
        general_map.setdefault(sid, []).append((ratio, to))
    else:
        specific_map.setdefault((sid, int(cid)), []).append((ratio, to))

# -----------------------------
# 8. 计算佣金
# -----------------------------
def calc_comm(r):
    base = (r['revenue_payment'] + r['revenue_refund']) * 0.04
    paid_sen = paid_tot = 0.0
    senior = 0
    role = r['sales_role_id']
    if role >= 0:
        rules = specific_map.get((role, r['customer_id'])) or general_map.get(role, [])
        for ratio, to in rules:
            p = base * (ratio / 100)
            paid_tot += p
            if to:
                paid_sen += p
                senior = to
    return pd.Series({
        'commission_from_revenue': base,
        'commission_paid_to_senior': paid_sen,
        'senior_sales_id': senior,
        'commission_paid_total': paid_tot,
        'commission_final': base - paid_tot
    })

comm_df = df.apply(calc_comm, axis=1)
df = pd.concat([df, comm_df], axis=1)
df['order_time'] = pd.to_datetime(df['created_date']).dt.strftime('%m/%d/%Y')

# -----------------------------
# 9. 导出 WellProz_details.xlsx
# -----------------------------
details = df.groupby(
    ['sample_id', 'patient_id', 'customer_id', 'customer_name', 'order_time'],
    as_index=False
).agg({
    'sales_role_id': 'first',
    'revenue_payment': 'sum',
    'revenue_refund': 'sum',
    'commission_from_revenue': 'sum',
    'commission_final': 'sum'
}).rename(columns={'sales_role_id': 'sales_id'})

with pd.ExcelWriter('WellProz_details.xlsx', engine='xlsxwriter') as writer:
    details.to_excel(writer, index=False, sheet_name='Details')
    worksheet = writer.sheets['Details']
    for col, width in {'A':12,'B':12,'C':12,'D':12,'E':20,'F':12,'G':18,'H':18,'I':18}.items():
        worksheet.set_column(f"{col}:{col}", width)

# -----------------------------
# 10. 导出 WellProz_summary.xlsx
# -----------------------------
summary = df.groupby('sales_user_id', as_index=False).agg({
    'commission_from_revenue': 'sum',
    'commission_paid_to_senior': 'sum',
    'commission_paid_total': 'sum',
    'commission_final': 'sum'
}).rename(columns={'sales_user_id': 'new_sales_id'})

with pd.ExcelWriter('WellProz_summary.xlsx', engine='xlsxwriter') as writer:
    summary.to_excel(writer, index=False, sheet_name='Summary')
    worksheet = writer.sheets['Summary']
    for col, width in {'A':12,'B':12,'C':20,'D':18,'E':18,'F':18,'G':18}.items():
        worksheet.set_column(f"{col}:{col}", width)

print("✅ WellProz 佣金计算完成")
