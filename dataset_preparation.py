# import csv
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

engine = create_engine('postgresql://<db engine here>')

max_cust = pd.read_sql_query('SELECT \
    d.district_name, \
    COUNT(DISTINCT c.client_id) as customer_count \
FROM district as d \
    JOIN client as c \
    ON d.district_id = c.district_id \
GROUP BY d.district_name \
ORDER BY customer_count DESC \
LIMIT 1',con=engine)
max_cust

gender = pd.read_sql_query("""
SELECT
    CASE
        WHEN CAST(substr(birth_number::text, 3,2) as integer) > 50 THEN \'WOMEN\'
    ELSE \'MAN\'
    END AS gender,
    COUNT(client_id) as customer_count
FROM client
GROUP BY gender
""",con=engine)
gender

client_age = pd.read_sql_query("""
    SELECT
        client_id,
        '19'||substr(birth_number::text, 1,2) as birthyear,
        EXTRACT( Year from DATE('now')) -CAST('19'||substr(birth_number::text, 1,2) as integer) as age
    FROM client
""", engine)
client_age = client_age.set_index('client_id')

popular_gender = pd.read_sql_query("""
SELECT
    CASE
        WHEN CAST(substr(cl.birth_number::text, 3,2) as integer) > 50 THEN 'WOMEN'
    ELSE 'MAN'
    END AS gender,
    c.type,
    COUNT(card_id) as card_count
FROM card as c
    JOIN relationship as r
    ON c.disp_id = r.disp_id
    JOIN client as cl
    ON r.client_id = cl.client_id
    JOIN district as d
    ON cl.district_id = d.district_id
GROUP BY gender, c.type
ORDER BY c.type
""", engine)

popular_agegroup = pd.read_sql_query("""
SELECT
    CASE
        WHEN EXTRACT( Year from DATE('now')) -CAST('19'||substr(birth_number::text, 1,2) as integer) <50 THEN 'Age 30-50'
        WHEN EXTRACT( Year from DATE('now')) -CAST('19'||substr(birth_number::text, 1,2) as integer) <70 THEN 'Age 50-70'
        WHEN EXTRACT( Year from DATE('now')) -CAST('19'||substr(birth_number::text, 1,2) as integer) <90 THEN 'Age 70-90'
    ELSE 'Age 90 and above'
    END as age_group,
    c.type,
    COUNT(card_id) as card_count
FROM card as c
    JOIN relationship as r
    ON c.disp_id = r.disp_id
    JOIN client as cl
    ON r.client_id = cl.client_id
    JOIN district as d
    ON cl.district_id = d.district_id
GROUP BY age_group, c.type
ORDER BY age_group, c.type
""", engine)

popular_region = pd.read_sql_query("""
SELECT
    region,
    c.type,
    COUNT(card_id) as card_count
FROM card as c
    JOIN relationship as r
    ON c.disp_id = r.disp_id
    JOIN client as cl
    ON r.client_id = cl.client_id
    JOIN district as d
    ON cl.district_id = d.district_id
GROUP BY region, c.type
ORDER BY c.type, card_count desc
""", engine)

trend_issuance = pd.read_sql_query("""
SELECT
    district_name,
    CAST('19'||substr(issued::text, 1,2) as integer) as year,
    c.type,
    COUNT(card_id) as card_count
FROM card as c
    JOIN relationship as r
    ON c.disp_id = r.disp_id
    JOIN client as cl
    ON r.client_id = cl.client_id
    JOIN district as d
    ON cl.district_id = d.district_id
GROUP BY district_name, year, c.type
""", engine)
