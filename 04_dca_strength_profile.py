import pandas as pd
import pandasql as psql

# Load data from CSV files
df_accounts = pd.read_csv('output_02_training_account_data.csv')
df_interactions = pd.read_csv('output_03_simulated_collection_interactions.csv')
df_customers = pd.read_csv('output_02_training_customer_profiles.csv')
df_dca_agents = pd.read_csv('output_01_fresh_dca_agent_data.csv')

# convert continuous variables to categorical for df_accounts days_in_collections
df_accounts['days_in_collections_category'] = pd.qcut(df_accounts['days_in_collections'], q=4, labels=['low', 'medium_low', 'medium_high', 'high'])


"""
# Print schema of accounts
print("Schema of accounts")
print(df_accounts.dtypes)

# Print schema of customers
print("Schema of customers")
print(df_customers.dtypes)

# Print schema of interactions
print("Schema of interactions")
print(df_interactions.dtypes)

# Print schema of dca agents
print("Schema of dca agents")
print(df_dca_agents.dtypes)
"""

# Get all of the phone call interactions, join on agent and account data, and customer data
df_01 = psql.sqldf("""
--sql
select
a.dca_id,
a.dca_name,
a.agent_that_called,
a.amount_recovered_percentage,
a.amount_recovered,
a.compliance_breached,
a.account,
c.product_code,
c.retail_or_business_flag,
c.days_in_collections,
c.days_in_collections_category,
d.age,
d.bankruptcy_type
from
df_interactions a
left join df_dca_agents b on b.dca_agent_id = a.agent_that_called
left join df_accounts c on c.account_number = a.account
left join df_customers d on d.customer_number = c.customer_number
where
a.type_of_interaction = 'phone call'
--endsql     
""")


# create the recovery rate of the agents based on various categories
df_02 = psql.sqldf("""
--sql
select
a.*,
round((total_compliance_breaches*1.0)/total_interactions,2) as compliance_breach_rate
from
(
    select
    dca_id,
    dca_name,
    product_code,
    retail_or_business_flag,
    days_in_collections_category,
    count(*) as total_interactions,
    sum(amount_recovered) as total_recovered,
    avg(amount_recovered_percentage) as avg_recovery_rate,
    sum(case when compliance_breached = 'Y' then 1 else 0 end) as total_compliance_breaches
    from
    df_01
    group by
    dca_id,
    dca_name,
    product_code,
    retail_or_business_flag,
    days_in_collections_category
    order by total_interactions desc
) a
--endsql
""")
# print schema of df_02


# now do the same for letters
df_03 = psql.sqldf("""
--sql
select
a.dca_id,
a.dca_name,
a.amount_recovered_percentage,
a.amount_recovered,
a.compliance_breached,
a.account,
c.product_code,
c.retail_or_business_flag,
c.days_in_collections,
c.days_in_collections_category,
d.age,
d.bankruptcy_type
from
df_interactions a
left join df_accounts c on c.account_number = a.account
left join df_customers d on d.customer_number = c.customer_number
where
a.type_of_interaction = 'letter'
--endsql     
""")

# create the recovery rate of the letters
df_04 = psql.sqldf("""
--sql
select
a.*,
round((total_compliance_breaches*1.0)/total_interactions,2) as compliance_breach_rate
from
(
    select
    dca_id,
    dca_name,
    product_code,
    retail_or_business_flag,
    days_in_collections_category,
    count(*) as total_interactions,
    sum(amount_recovered) as total_recovered,
    avg(amount_recovered_percentage) as avg_recovery_rate,
    sum(case when compliance_breached = 'Y' then 1 else 0 end) as total_compliance_breaches
    from
    df_03
    group by
    dca_id,
    dca_name,
    product_code,
    retail_or_business_flag,
    days_in_collections_category
    order by total_interactions desc
) a
--endsql
""")

# combine the phone call and letter data
df_05 = psql.sqldf("""
--sql
select
'phone call' as type_of_interaction,
dca_id,
dca_name,
product_code,
retail_or_business_flag,
days_in_collections_category,
total_interactions,
-- just how i synthesized the data
round(total_recovered,2) as total_recovered,
avg_recovery_rate,
compliance_breach_rate
from
df_02

union

select
'letter' as type_of_interaction,
dca_id,
dca_name,
product_code,
retail_or_business_flag,
days_in_collections_category,
total_interactions,
round(total_recovered,2) as total_recovered,
avg_recovery_rate,
compliance_breach_rate
from
df_04
--endsql
""")



# Save to CSV
df_05.to_csv('output_04_dca_strength_profiles.csv', index=False)
