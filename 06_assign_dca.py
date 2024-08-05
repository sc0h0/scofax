import pandas as pd
import pandasql as psql
from tabulate import tabulate

# Load data from CSV files
df_accounts = pd.read_csv('output_05_campaign_account_data.csv')
df_customers = pd.read_csv('output_05_campaign_customer_profiles.csv')

# convert continuous variables to categorical for df_accounts days_in_collections
df_accounts['days_in_collections_category'] = pd.qcut(df_accounts['days_in_collections'], q=4, labels=['low', 'medium_low', 'medium_high', 'high'])

# load in strength profile data
df_dca_strength_profiles = pd.read_csv('output_04_dca_strength_profiles.csv')
# print schema of dca strength profiles
print("Schema of dca strength profiles")
print(df_dca_strength_profiles.dtypes)


# create another dataframe with average recovery rate and compliance breach rate
df_dca_stats = psql.sqldf("""
--sql
select
avg(avg_recovery_rate) as avg_recovery_rate,
avg(compliance_breach_rate) as compliance_breach_rate
from df_dca_strength_profiles
--endsql
""")

df_01 = psql.sqldf("""
--sql
select
a.account_number,
a.customer_number,
a.product_code,
a.retail_or_business_flag,
a.current_total_balance,
a.days_in_collections_category
from
df_accounts a

left join df_customers b on b.customer_number = a.customer_number

--endsql     
""")

# print schema of df_01
print("Schema of df_01")
print(df_01.dtypes)
# count rows of df_01
print("Count of df_01") 
print(len(df_01))

df_02 = psql.sqldf("""
--sql
SELECT 
df_01.*,
dca.dca_id,
dca.type_of_interaction,
dca.avg_recovery_rate,
dca.compliance_breach_rate
FROM df_01
LEFT JOIN df_dca_strength_profiles dca
ON df_01.product_code = dca.product_code
AND df_01.retail_or_business_flag = dca.retail_or_business_flag
AND df_01.days_in_collections_category = dca.days_in_collections_category
--endsql
""")
print(len(df_02))

df_03 = psql.sqldf("""
--sql
SELECT
account_number,
customer_number,
product_code,
retail_or_business_flag,
current_total_balance,
days_in_collections_category,

MAX(CASE WHEN dca_id = 1 THEN 1 ELSE 0 END) AS dca_1_match,
MAX(CASE WHEN dca_id = 1 AND type_of_interaction = 'letter' THEN avg_recovery_rate END) AS dca_1_letter_avg_recovery_rate,
MAX(CASE WHEN dca_id = 1 AND type_of_interaction = 'letter' THEN compliance_breach_rate END) AS dca_1_letter_compliance_breach_rate,
MAX(CASE WHEN dca_id = 1 AND type_of_interaction = 'phone call' THEN avg_recovery_rate END) AS dca_1_call_avg_recovery_rate,
MAX(CASE WHEN dca_id = 1 AND type_of_interaction = 'phone call' THEN compliance_breach_rate END) AS dca_1_call_compliance_breach_rate,

MAX(CASE WHEN dca_id = 2 THEN 1 ELSE 0 END) AS dca_2_match,
MAX(CASE WHEN dca_id = 2 AND type_of_interaction = 'letter' THEN avg_recovery_rate END) AS dca_2_letter_avg_recovery_rate,
MAX(CASE WHEN dca_id = 2 AND type_of_interaction = 'letter' THEN compliance_breach_rate END) AS dca_2_letter_compliance_breach_rate,
MAX(CASE WHEN dca_id = 2 AND type_of_interaction = 'phone call' THEN avg_recovery_rate END) AS dca_2_call_avg_recovery_rate,
MAX(CASE WHEN dca_id = 2 AND type_of_interaction = 'phone call' THEN compliance_breach_rate END) AS dca_2_call_compliance_breach_rate,

MAX(CASE WHEN dca_id = 3 THEN 1 ELSE 0 END) AS dca_3_match,
MAX(CASE WHEN dca_id = 3 AND type_of_interaction = 'letter' THEN avg_recovery_rate END) AS dca_3_letter_avg_recovery_rate,
MAX(CASE WHEN dca_id = 3 AND type_of_interaction = 'letter' THEN compliance_breach_rate END) AS dca_3_letter_compliance_breach_rate,
MAX(CASE WHEN dca_id = 3 AND type_of_interaction = 'phone call' THEN avg_recovery_rate END) AS dca_3_call_avg_recovery_rate,
MAX(CASE WHEN dca_id = 3 AND type_of_interaction = 'phone call' THEN compliance_breach_rate END) AS dca_3_call_compliance_breach_rate,

MAX(CASE WHEN dca_id = 4 THEN 1 ELSE 0 END) AS dca_4_match,
MAX(CASE WHEN dca_id = 4 AND type_of_interaction = 'letter' THEN avg_recovery_rate END) AS dca_4_letter_avg_recovery_rate,
MAX(CASE WHEN dca_id = 4 AND type_of_interaction = 'letter' THEN compliance_breach_rate END) AS dca_4_letter_compliance_breach_rate,
MAX(CASE WHEN dca_id = 4 AND type_of_interaction = 'phone call' THEN avg_recovery_rate END) AS dca_4_call_avg_recovery_rate,
MAX(CASE WHEN dca_id = 4 AND type_of_interaction = 'phone call' THEN compliance_breach_rate END) AS dca_4_call_compliance_breach_rate,

MAX(CASE WHEN dca_id = 5 THEN 1 ELSE 0 END) AS dca_5_match,
MAX(CASE WHEN dca_id = 5 AND type_of_interaction = 'letter' THEN avg_recovery_rate END) AS dca_5_letter_avg_recovery_rate,
MAX(CASE WHEN dca_id = 5 AND type_of_interaction = 'letter' THEN compliance_breach_rate END) AS dca_5_letter_compliance_breach_rate,
MAX(CASE WHEN dca_id = 5 AND type_of_interaction = 'phone call' THEN avg_recovery_rate END) AS dca_5_call_avg_recovery_rate,
MAX(CASE WHEN dca_id = 5 AND type_of_interaction = 'phone call' THEN compliance_breach_rate END) AS dca_5_call_compliance_breach_rate

FROM df_02 
GROUP BY 
account_number,
customer_number,
product_code,
retail_or_business_flag,
current_total_balance,
days_in_collections_category

--endsql
""")
print(len(df_03))
# schema
print("Schema of df_03")
print(df_03.dtypes)
print(tabulate(df_03.head(10), headers='keys', tablefmt='psql'))

psql.sqldf("""
--sql
select
sum(dca_1_match) as dca_1_match,
sum(dca_2_match) as dca_2_match,
sum(dca_3_match) as dca_3_match,
sum(dca_4_match) as dca_4_match,
sum(dca_5_match) as dca_5_match
from
df_03
--endsql
""")

df_04 = psql.sqldf("""
--sql
select
a.*,
current_total_balance * avg_recovery_rate/100.0 as avg_expected_return
from
(
    SELECT 
    *,
    current_total_balance * dca_1_letter_avg_recovery_rate / 100.0 AS dca_1_letter_expected_return,
    current_total_balance * dca_1_call_avg_recovery_rate / 100.0 AS dca_1_call_expected_return,
    current_total_balance * dca_2_letter_avg_recovery_rate / 100.0 AS dca_2_letter_expected_return,
    current_total_balance * dca_2_call_avg_recovery_rate / 100.0 AS dca_2_call_expected_return,
    current_total_balance * dca_3_letter_avg_recovery_rate / 100.0 AS dca_3_letter_expected_return,
    current_total_balance * dca_3_call_avg_recovery_rate / 100.0 AS dca_3_call_expected_return,
    current_total_balance * dca_4_letter_avg_recovery_rate / 100.0 AS dca_4_letter_expected_return,
    current_total_balance * dca_4_call_avg_recovery_rate / 100.0 AS dca_4_call_expected_return,
    current_total_balance * dca_5_letter_avg_recovery_rate / 100.0 AS dca_5_letter_expected_return,
    current_total_balance * dca_5_call_avg_recovery_rate / 100.0 AS dca_5_call_expected_return,
    d.*
    FROM df_03 

    cross join df_dca_stats d
) a
--endsql
""")
print(len(df_04))
print(tabulate(df_04.head(10), headers='keys', tablefmt='psql'))

df_05 = psql.sqldf("""
--sql
select
account_number,
customer_number,
product_code,
retail_or_business_flag,
current_total_balance,
days_in_collections_category,
1 as dca_id,
dca_1_letter_avg_recovery_rate as letter_avg_recovery_rate,
dca_1_letter_compliance_breach_rate as letter_avg_compliance_breach_rate,
dca_1_call_avg_recovery_rate as call_avg_recovery_rate,
dca_1_call_compliance_breach_rate as call_avg_compliance_breach_rate,
dca_1_letter_expected_return as letter_expected_return,
dca_1_call_expected_return as call_expected_return
from
df_04
where dca_1_match = 1
union
select
account_number,
customer_number,
product_code,
retail_or_business_flag,
current_total_balance,
days_in_collections_category,
2 as dca_id,
dca_2_letter_avg_recovery_rate as letter_avg_recovery_rate,
dca_2_letter_compliance_breach_rate as letter_avg_compliance_breach_rate,
dca_2_call_avg_recovery_rate as call_avg_recovery_rate,
dca_2_call_compliance_breach_rate as call_avg_compliance_breach_rate,
dca_2_letter_expected_return as letter_expected_return,
dca_2_call_expected_return as call_expected_return
from
df_04
where dca_2_match = 1
union
select
account_number,
customer_number,
product_code,
retail_or_business_flag,
current_total_balance,
days_in_collections_category,
3 as dca_id,
dca_3_letter_avg_recovery_rate as letter_avg_recovery_rate,
dca_3_letter_compliance_breach_rate as letter_avg_compliance_breach_rate,
dca_3_call_avg_recovery_rate as call_avg_recovery_rate,
dca_3_call_compliance_breach_rate as call_avg_compliance_breach_rate,
dca_3_letter_expected_return as letter_expected_return,
dca_3_call_expected_return as call_expected_return
from
df_04
where dca_3_match = 1
union
select
account_number,
customer_number,
product_code,
retail_or_business_flag,
current_total_balance,
days_in_collections_category,
4 as dca_id,
dca_4_letter_avg_recovery_rate as letter_avg_recovery_rate,
dca_4_letter_compliance_breach_rate as letter_avg_compliance_breach_rate,
dca_4_call_avg_recovery_rate as call_avg_recovery_rate,
dca_4_call_compliance_breach_rate as call_avg_compliance_breach_rate,
dca_4_letter_expected_return as letter_expected_return,
dca_4_call_expected_return as call_expected_return
from
df_04
where dca_4_match = 1
union
select
account_number,
customer_number,
product_code,
retail_or_business_flag,
current_total_balance,
days_in_collections_category,
5 as dca_id,
dca_5_letter_avg_recovery_rate as letter_avg_recovery_rate,
dca_5_letter_compliance_breach_rate as letter_avg_compliance_breach_rate,
dca_5_call_avg_recovery_rate as call_avg_recovery_rate,
dca_5_call_compliance_breach_rate as call_avg_compliance_breach_rate,
dca_5_letter_expected_return as letter_expected_return,
dca_5_call_expected_return as call_expected_return
from
df_04
where dca_5_match = 1;
--endsql
""")
print(len(df_05))
print(tabulate(df_05.head(10), headers='keys', tablefmt='psql'))

# join on the dca_name
# load in the 
df_06 = psql.sqldf("""
--sql
select
a.*,
b.dca_name
from
df_05 a
left join (select distinct dca_id, dca_name from df_dca_strength_profiles) b on a.dca_id = b.dca_id
--endsql
""")

# write to csv
df_06.to_csv('output_06_dca_assignment.csv', index=False)