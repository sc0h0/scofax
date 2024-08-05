import pandas as pd
from faker import Faker
import random

# Initialize Faker
fake = Faker()

# Function to generate DCA and agent data
def generate_dca_agent_data(num_dca, min_agents, max_agents):
    agent_data = []
    
    for dca_id in range(1, num_dca + 1):
        dca_name = fake.company()
        num_agents = random.randint(min_agents, max_agents)
        
        for _ in range(num_agents):
            agent = {
                'dca_id': dca_id,
                'dca_name': dca_name,
                'dca_agent_id': fake.uuid4()
            }
            agent_data.append(agent)
    
    return agent_data

# Generate Data
num_dca = 5
min_agents = 10
max_agents = 30

dca_agent_data = generate_dca_agent_data(num_dca, min_agents, max_agents)

# Convert to DataFrame
df_dca_agents = pd.DataFrame(dca_agent_data)

# Save to CSV
df_dca_agents.to_csv('output_01_fresh_dca_agent_data.csv', index=False)

print("Data generation complete. File saved as 'dca_agent_data.csv'.")
