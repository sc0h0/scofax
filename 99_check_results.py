import pandas as pd
import pandasql as psql
from tabulate import tabulate

df_dca_agents = pd.read_csv('output_01_fresh_dca_agent_data.csv')
df_accounts = pd.read_csv('output_02_training_account_data.csv')
df_customers = pd.read_csv('output_02_training_customer_profiles.csv')
df_interactions = pd.read_csv('output_03_simulated_collection_interactions.csv')
df_dca_strengths = pd.read_csv('output_04_dca_strength_profiles.csv')


# how many unique agents are there in the interactions data? print the result
print(df_interactions['agent_that_called'].nunique())

# how many unique agents in the dca agent data
print(df_dca_agents['dca_agent_id'].nunique())

# print unique customers
print(df_customers['customer_number'].nunique())
# print unique accounts
print(df_accounts['account_number'].nunique())
# print unique interactions split by phone call and letter
print(df_interactions['type_of_interaction'].value_counts())
150418 + 149700
# total
print(df_interactions['type_of_interaction'].count())

# get average interacations per agent
psql.sqldf("""
--sql
select
agent_that_called,
count(*) as num_interactions
from 
df_interactions
where type_of_interaction = 'phone call'
group by agent_that_called
order by num_interactions desc
--endsql
""")

# print a sample of interactions 
from tabulate import tabulate
print(tabulate(df_interactions.sample(10), headers='keys', tablefmt='psql'))

# print a sample of dca strengths. # drop total_recovered
df_dca_strengths.drop(columns=['total_recovered'], inplace=True)
print(tabulate(df_dca_strengths.sample(10), headers='keys', tablefmt='psql'))


# open output_06_dca_assignment.csv
df_dca_assignment = pd.read_csv('output_06_dca_assignment.csv')
# print a sample of dca assignment
print(tabulate(df_dca_assignment.head(10), headers='keys', tablefmt='psql'))
# print but translate so that rows are columns for account_number = AABE15050314767239
print(tabulate(df_dca_assignment[df_dca_assignment['account_number'] == 'AABE15050314767239'].T, headers='keys', tablefmt='psql'))

df_01 = psql.sqldf("""
--sql
select 
t1.account_number,
t1.dca_name,
call_expected_return,
call_avg_compliance_breach_rate
from
(
    select 
    account_number,
    dca_name,
    call_expected_return,
    call_avg_compliance_breach_rate,
    row_number() over (partition by account_number order by call_expected_return desc) as row_num
    from 
    df_dca_assignment
) t1
where 
t1.row_num = 1
--endsql
""")

df_02 = psql.sqldf("""
--sql
select
dca_name,
count(*) as num_accounts,
round(sum(call_expected_return)/1000000,2) as total_expected_return_M,
cast(sum(call_avg_compliance_breach_rate) as int) as total_compliance_breach
from
df_01
group by
dca_name
--endsql
""")
pd.set_option('display.float_format', lambda x: '%.3f' % x)
print(tabulate(df_02, headers='keys', tablefmt='psql'))
# total accounts in df_02
print(df_02['num_accounts'].sum())

# do the same for letters
df_03 = psql.sqldf("""
--sql
select 
t1.account_number,
t1.dca_name,
letter_expected_return
from
(
    select 
    account_number,
    dca_name,
    letter_expected_return,
    row_number() over (partition by account_number order by letter_expected_return desc) as row_num
    from 
    df_dca_assignment
) t1
where 
t1.row_num = 1
--endsql
""")

df_04 = psql.sqldf("""
--sql
select
dca_name,
count(*) as num_accounts,
round(sum(letter_expected_return)/1000000,2) as total_expected_return_M
from
df_03
group by
dca_name
--endsql
""")

print(tabulate(df_04, headers='keys', tablefmt='psql'))