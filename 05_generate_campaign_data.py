import pandas as pd
from faker import Faker
import random

# Initialize Faker
fake = Faker('en_AU')

# Function to generate customer profiles
def generate_customer_profiles(num_customers):
    customers = []
    for _ in range(num_customers):
        customer_number = fake.uuid4()
        profile = {
            'customer_number': customer_number,
            'full_name': fake.name(),
            'gender': fake.random_element(elements=('Male', 'Female')),
            'random_postcode': fake.postcode(),
            'age': fake.random_int(min=18, max=90),
            'bankruptcy_type': fake.random_element(elements=('IX', 'X', 'FULL'))
        }
        customers.append(profile)
    return customers

# Function to generate account data
def generate_account_data(customers):
    accounts = []
    product_codes = ['CC', 'PL', 'DDA', 'HL']
    industry_codes = [
        'Agriculture, Forestry and Fishing',
        'Mining',
        'Manufacturing',
        'Construction',
        'Retail Trade',
        'Accommodation and Food Services',
        'Transport, Postal and Warehousing',
        'Financial and Insurance Services',
        'Professional, Scientific and Technical Services',
        'Health Care and Social Assistance'
    ]
    for customer in customers:
        num_accounts = random.randint(1, 5)  # Each customer has between 1 and 5 accounts
        for _ in range(num_accounts):
            account_number = fake.bban()
            current_total_balance = round(fake.random_number(digits=5, fix_len=True) / 100, 2)
            product_credit_limit = round(random.uniform(0, current_total_balance), 2)
            retail_or_business_flag = fake.random_element(elements=('Retail', 'Business'))
            industry_code = 'NA' if retail_or_business_flag == 'Retail' else random.choice(industry_codes)
            account = {
                'account_number': account_number,
                'customer_number': customer['customer_number'],
                'product_code': random.choice(product_codes),
                'retail_or_business_flag': retail_or_business_flag,
                'industry_code': industry_code,
                'current_total_balance': current_total_balance,
                'product_credit_limit': product_credit_limit,
                'days_in_collections': fake.random_int(min=0, max=365)
            }
            accounts.append(account)
            customer['account_number'] = account_number  # Ensuring the last account number is associated with the customer
    return accounts

# Generate Data
num_customers = 10000

customer_profiles = generate_customer_profiles(num_customers)
account_data = generate_account_data(customer_profiles)

# Convert to DataFrame
df_customers = pd.DataFrame(customer_profiles)
df_accounts = pd.DataFrame(account_data)

# Save to CSV
df_customers.to_csv('output_05_campaign_customer_profiles.csv', index=False)
df_accounts.to_csv('output_05_campaign_account_data.csv', index=False)


