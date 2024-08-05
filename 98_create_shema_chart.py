import pandas as pd
import pandasql as psql

df_dca_agents = pd.read_csv('output_01_fresh_dca_agent_data.csv')
df_accounts = pd.read_csv('output_02_training_account_data.csv')
df_customers = pd.read_csv('output_02_training_customer_profiles.csv')
df_interactions = pd.read_csv('output_03_simulated_collection_interactions.csv')

# print schema of all tables
print('Schema of df_dca_agents:')
print(df_dca_agents.dtypes)
print('Schema of df_accounts:')
print(df_accounts.dtypes)
print('Schema of df_customers:')
print(df_customers.dtypes)
print('Schema of df_interactions:')
print(df_interactions.dtypes)

from graphviz import Digraph


# Initialize a Digraph with adjusted aspect ratio
dot = Digraph()

# Define a function to create a node with stacked fields
def add_table_node(dot, node_name, title, fields):
    label = f"""<
        <TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0">
            <TR><TD BGCOLOR="lightgrey">{title}</TD></TR>
            {''.join([f'<TR><TD>{field}</TD></TR>' for field in fields])}
        </TABLE>
    >"""
    dot.node(node_name, label=label)

# Add nodes for each DataFrame with stacked fields
add_table_node(dot, 'DCA_Agents', 'df_dca_agents', ['dca_id', 'dca_name', 'dca_agent_id'])
add_table_node(dot, 'Accounts', 'df_accounts', ['account_number', 'customer_number', 'product_code', 'retail_or_business_flag', 'industry_code', 'current_total_balance', 'product_credit_limit', 'days_in_collections'])
add_table_node(dot, 'Customers', 'df_customers', ['customer_number', 'full_name', 'gender', 'random_postcode', 'age', 'bankruptcy_type', 'account_number'])
add_table_node(dot, 'Interactions', 'df_interactions', ['account', 'type_of_interaction', 'dca_id', 'dca_name', 'agent_that_called', 'amount_recovered_percentage', 'amount_recovered', 'compliance_breached'])

# Add edges to represent relationships
dot.edge('Accounts', 'Customers', 'customer_number')
dot.edge('Interactions', 'DCA_Agents', 'dca_agent_id')
dot.edge('Interactions', 'Accounts', 'account_number')

# Render the graph
dot.render('data_schema_vertical', format='png', cleanup=True)

# Display the graph in Jupyter Notebook (if applicable)
from IPython.display import Image
Image('data_schema_vertical.png')