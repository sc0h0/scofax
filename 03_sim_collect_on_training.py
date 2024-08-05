import pandas as pd
import random

# Load data from CSV files
df_dca_agents = pd.read_csv('output_01_fresh_dca_agent_data.csv')
df_accounts = pd.read_csv('output_02_training_account_data.csv')

# Assign random bias to DCAs and agents
dca_phone_call_bias = {dca_id: random.uniform(0.5, 1.5) for dca_id in df_dca_agents['dca_id'].unique()}  # Bias towards phone call efficiency
agent_phone_call_efficiency = {agent_id: random.uniform(0.5, 1.5) for agent_id in df_dca_agents['dca_agent_id']}
dca_compliance_bias = {dca_id: random.uniform(0.01, 0.1) for dca_id in df_dca_agents['dca_id'].unique()}  # Bias for compliance breach

# Function to adjust percentage recovery based on bias
def adjust_percentage(base_percentage, efficiency):
    if base_percentage == 0:
        return 0
    if base_percentage == 50:
        return 50 if efficiency >= 1 else 0
    if base_percentage == 100:
        return 100 if efficiency >= 1 else 50

# Function to simulate collection interactions
def simulate_collections(df_accounts, df_dca_agents):
    interactions = []
    interaction_types = ['phone call', 'letter']
    contacted_accounts = set()
    
    for index, account in df_accounts.iterrows():
        if account['account_number'] in contacted_accounts:
            continue
        
        interaction_type = random.choice(interaction_types)
        if interaction_type == 'phone call':
            agent = df_dca_agents.sample(1).iloc[0]  # Randomly select one DCA agent
            agent_id = agent['dca_agent_id']
            dca_id = agent['dca_id']
            dca_name = agent['dca_name']
            compliance_chance = dca_compliance_bias[dca_id]
            compliance_breached = 'Y' if random.random() < compliance_chance else 'N'
            
            # Adjust recovery based on agent's efficiency and DCA's phone call bias
            base_recovery_percentage = random.choice([0, 50, 100])
            dca_efficiency = dca_phone_call_bias[dca_id]
            adjusted_recovery_percentage = adjust_percentage(base_recovery_percentage, agent_phone_call_efficiency[agent_id] * dca_efficiency)
        
        else:
            agent_id = 'NA'
            agent = df_dca_agents.sample(1).iloc[0]  # Randomly select one DCA for the letter
            dca_id = agent['dca_id']
            dca_name = agent['dca_name']
            compliance_breached = 'N'
            
            # Adjust recovery based on DCA's bias towards phone calls
            base_recovery_percentage = random.choice([0, 50, 100])
            if dca_phone_call_bias[dca_id] > 1:
                adjusted_recovery_percentage = random.choice([0, 25])
            else:
                adjusted_recovery_percentage = random.choice([0, 50])
        
        amount_recovered = adjusted_recovery_percentage * account['current_total_balance'] / 100
        
        interaction = {
            'account': account['account_number'],
            'type_of_interaction': interaction_type,
            'dca_id': dca_id,
            'dca_name': dca_name,
            'agent_that_called': agent_id,
            'amount_recovered_percentage': adjusted_recovery_percentage,
            'amount_recovered': amount_recovered,
            'compliance_breached': compliance_breached
        }
        interactions.append(interaction)
        contacted_accounts.add(account['account_number'])  # Mark the account as contacted
    
    return interactions

# Perform the simulation
collection_interactions = simulate_collections(df_accounts, df_dca_agents)

# Convert to DataFrame
df_interactions = pd.DataFrame(collection_interactions)

# Save to CSV
df_interactions.to_csv('output_03_simulated_collection_interactions.csv', index=False)

print("Simulation complete. Results saved as 'output_03_simulated_collection_interactions.csv'.")
