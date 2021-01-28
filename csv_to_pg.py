# import csv
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

engine = create_engine('postgresql://<db engine here>')
#District
district = pd.read_csv('district.csv')
district.columns = [
    'district_id',
    'district_name',
    'region',
    'no_inhabitants',
    'no_municipalities_1',
    'no_municipalities_2',
    'no_municipalities_3',
    'no_municipalities_4',
    'no_cities',
    'ratio_urban_inhabitants',
    'avg_salary',
    'unemployment_rate_95',
    'unemployment_rate_96',
    'entrepreneurs_per_1000_inhabitants',
    'no_crimes_95',
    'no_crimes_96']
district = district.set_index('district_id')
district.to_sql('district', con=engine, if_exists='replace')
# read relationship.csv
relationship = pd.read_csv('relationship.csv', sep=';')
relationship = relationship.set_index('disp_id')
# write to db
relationship.to_sql('relationship', engine, if_exists='replace')

# read loan.csv
loan = pd.read_csv('loan.csv', sep=';')
loan = loan.set_index('loan_id')
# write to db
loan.to_sql('loan', engine, if_exists='replace')

# read trans.csv
trans = pd.read_csv('trans.csv', sep=';')
trans = trans.set_index('trans_id')
# write to db
trans.to_sql('trans', engine, if_exists='replace')

# read order.csv
order = pd.read_csv('order.csv')
order = order.set_index('order_id')
# write to db
order.to_sql('orderlist', engine, if_exists='replace')

# read client.csv
client = pd.read_csv('client.csv', sep=';')
client = client.set_index('client_id')
# write to db
client.to_sql('client', engine, if_exists='replace')

# read card.csv
card = pd.read_csv('card.csv', sep=';')
card = card.set_index('card_id')
# write to db
card.to_sql('card', engine, if_exists='replace')

# read account.csv
account = pd.read_csv('account.csv', sep=';')
account = account.set_index('account_id')
# write to db
account.to_sql('account', engine, if_exists='replace')
