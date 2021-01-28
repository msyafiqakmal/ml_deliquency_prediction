import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from imblearn.under_sampling import RandomUnderSampler
from imblearn.over_sampling import SMOTE, RandomOverSampler
from sklearn.model_selection import train_test_split
from numpy import savetxt

engine = create_engine('postgresql://<db engine here>')

merged_data_cleaner = pd.read_sql_query("""
SELECT a.account_id,
       a.date                                              as acc_create_date,
       case
           when a.frequency ilike 'Poplatek Mesicne' then 'monthly issuance'
           when a.frequency ilike 'Poplatek Tydne' then 'weekly issuance'
           when a.frequency ilike 'Poplatek Po Obratu' then 'issuance after transaction'
           end                                             as acc_statement_issue_frequency,
       l.date                                              as loan_date,
       l.amount                                            as loan_amount,
       l.duration                                          as loan_duration,
       l.payments                                          as loan_payments,
       l.status                                            as loan_status,
       district_name,
       region,
       no_inhabitants,
       no_municipalities_1,
       no_municipalities_2,
       no_municipalities_3,
       no_municipalities_4,
       no_cities,
       ratio_urban_inhabitants,
       avg_salary,
       case
           when unemployment_rate_95 ilike '?' then 0
           else unemployment_rate_95::double precision end as unemployment_rate_95,
       unemployment_rate_96,
       entrepreneurs_per_1000_inhabitants,
       case
           when no_crimes_95 ilike '?' then 0
           else no_crimes_95::double precision end         as no_crimes_95,
       no_crimes_96,
       num_trans,
       total_actual_amount,
       min_balance,
       type_credit,
       type_withdrawal,
       operation_cc_withdrawal,
       operation_credit_in_cash,
       operation_col_other_bank,
       operation_withdrawal_cash,
       operation_remittance_other_bank,
       operation_interest_credit,
       account_type,
       case
           when t2.amount is null then 0
           else t2.amount end                              as amount,
       case
           when t2.balance is null then 0
           else t2.balance end                             as balance,
       o.bank_to                                           as order_bank_recipient,
       o.account_to                                        as order_acc_recipient,
       o.amount                                            as order_amount,
       case
           when o.k_symbol ilike 'Pojistne' then 'insurance payment'
           when o.k_symbol ilike 'Sipo' then 'household payment'
           when o.k_symbol ilike 'Leasing' then 'leasing'
           when o.k_symbol ilike 'Uver' then 'loan payment'
           else 'others'
           end                                             as order_k_symbol,
       r.type                                              as disp_type,
       cl.birth_number                                     as client_birthdate,
       case
           when c.type is not null then 'card issued'
           else 'no card issued' end                       as card_type
FROM loan as l
         LEFT JOIN account as a
                   ON l.account_id = a.account_id
         LEFT JOIN district as d
                   ON a.district_id = d.district_id

         LEFT JOIN orderlist as o
                   ON a.account_id = o.account_id
         LEFT JOIN relationship as r
                   ON a.account_id = r.account_id
         LEFT JOIN client as cl
                   ON r.client_id = cl.client_id
         LEFT JOIN card as c
                   on r.disp_id = c.disp_id
         left join trans as t2 on a.account_id = t2.amount
         LEFT JOIN (
    select account_id,
           count(*)                             as num_trans,
           sum(actual_amount)                   as total_actual_amount,
           min(balance)                         as min_balance,
           sum(type_credit)                     as type_credit,
           sum(type_withdrawal)                 as type_withdrawal,
           sum(operation_cc_withdrawal)         as operation_cc_withdrawal,
           sum(operation_credit_in_cash)        as operation_credit_in_cash,
           sum(operation_col_other_bank)        as operation_col_other_bank,
           sum(operation_withdrawal_cash)       as operation_withdrawal_cash,
           sum(operation_remittance_other_bank) as operation_remittance_other_bank,
           sum(operation_interest_credit)       as operation_interest_credit,
           case
               when count(distinct bank) > 0 then 'Partner'
               else 'Single' end                as account_type
    from (
             select *,
                    case
                        when type ilike 'prijem' then amount
                        else -1 * amount end                                     as actual_amount,
                    case when type ilike 'Prijem' then 1 else 0 end              as type_credit,
                    case when type ilike 'Vydaj' then 1 else 0 end               as type_withdrawal,
                    case when type ilike 'Vyber' then 1 else 0 end               as type_withdrawal_cash,
                    case when operation ilike 'Vyber Kartou' then 1 else 0 end   as operation_cc_withdrawal,
                    case when operation ilike 'Vklad' then 1 else 0 end          as operation_credit_in_cash,
                    case when operation ilike 'Prevod Z Uctu' then 1 else 0 end  as operation_col_other_bank,
                    case when operation ilike 'Vyber' then 1 else 0 end          as operation_withdrawal_cash,
                    case when operation ilike 'Prevod Na Ucet' then 1 else 0 end as operation_remittance_other_bank,
                    case when operation is null then 1 else 0 end                as operation_interest_credit
             from trans) a
    group by account_id
) as t
                   ON a.account_id = t.account_id;
""",engine)

merged_data_cleaner.loc[merged_data_cln["loan_status"] =='A',"Defaulter"] = 0
merged_data_cleaner.loc[merged_data_cln["loan_status"] =='B',"Defaulter"] = 1
merged_data_cleaner.loc[merged_data_cln["loan_status"] =='C',"Defaulter"] = 0
merged_data_cleaner.loc[merged_data_cln["loan_status"] =='D',"Defaulter"] = 1

# break data into categorical and numerical
categorical_columns = [c for c in merged_data_cleaner.columns
                       if (merged_data_cleaner[c].dtypes.name == 'object') or ('date' in c or 'id' in c)]
numerical_columns = [c for c in merged_data_cleaner.columns
                     if merged_data_cleaner[c].dtypes.name != 'object' and ('date' not in c and 'id' not in c)]

new_categorical_columns = [c for c in categorical_columns if (c != 'loan_status') and ('date' not in c)]
new_categorical_columns.extend(numerical_columns)

final_dataframe = merged_data_cleaner.filter(new_categorical_columns)
final_dataframe = final_dataframe.set_index('account_id')

# get dummies!!!
dummified_final = pd.get_dummies(final_dataframe)

dummified_final.columns = dummified_final.columns.str.replace(' ', '')
dummified_final.columns = dummified_final.columns.str.replace('.', '')

dummified_final.to_csv('final/final_dataframe.csv',index = None)

x = dummified_final.drop(columns=['Defaulter'])
y = dummified_final.loc[:,'Defaulter']

# Sampling
rus = RandomUnderSampler(random_state=42)
ros = RandomOverSampler(random_state=42)
rsm = SMOTE(random_state=42)

x_rus, y_rus = rus.fit_resample(x, y)
x_ros, y_ros = ros.fit_resample(x, y)
x_rsm, y_rsm = rsm.fit_resample(x, y)

dummy_new_col = list(dummified_final.columns)
dummy_new_col.remove('Defaulter')

rus = pd.DataFrame(data=x_rus[0:,0:],
                  index=[i for i in range(x_rus.shape[0])],
                  columns=dummy_new_col)
rus = rus.join(pd.DataFrame(y_rus, columns=['Defaulter']) )

ros = pd.DataFrame(data=x_ros[0:,0:],
                  index=[i for i in range(x_ros.shape[0])],
                  columns=dummy_new_col)
ros = ros.join(pd.DataFrame(y_ros, columns=['Defaulter']) )

rsm = pd.DataFrame(data=x_rsm[0:,0:],
                  index=[i for i in range(x_rsm.shape[0])],
                  columns=dummy_new_col)
rsm = rsm.join(pd.DataFrame(y_rsm, columns=['Defaulter']) )

rus.to_csv('imbalanced/rus.csv',index = None)
ros.to_csv('imbalanced/ros.csv',index = None)
rsm.to_csv('imbalanced/rsm.csv',index = None)
