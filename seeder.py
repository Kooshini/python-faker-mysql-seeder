# An example how to use Faker to create fake data and inject them in a mysql database
import time
import os
import mysql.connector
from mysql.connector import Error
from faker import Faker
Faker.seed(33422)

fake = Faker()

db_host = os.environ.get('DB_HOST', 'localhost')
db_name = os.environ.get('DB_NAME', 'lead_logger')
db_user = os.environ.get('DB_USER_NAME', 'root')
db_pass = os.environ.get('DB_USER_PASSWORD', '')

# number of leads to be inserted per iteration
batch_size = 100

try:
    conn = mysql.connector.connect(host=db_host, database=db_name,
                                   user=db_user, password=db_pass)

    if conn.is_connected():
        cursor = conn.cursor()
        n = 0

    while True:
        n += 1

        client_id = fake.random_int(min=1, max=39)
        source_platform = fake.random_element(
            elements=('leadshook', 'facebook-form'))
        utm_ad_id = fake.random_number(
            digits=8, fix_len=True)
        source_lead_id = fake.random_number(
            digits=10, fix_len=True)
        source_ip = fake.ipv4()
        source_device = fake.random_element(
            elements=('desktop', 'mobile'))
        source_ismobile = fake.random_element(
            elements=('true', 'false'))
        source_os = fake.random_element(
            elements=('windows', 'mac', 'linux'))
        utm_source = fake.random_element(
            elements=('google', 'bing', 'yahoo', 'facebook'))
        created_at = fake.date_time_this_year()

        try:
            sql = "INSERT INTO `leads` (`id`, `client_id`, `source_platform`, `utm_ad_id`, `source_lead_id`, `source_ip`, `source_device`, `source_ismobile`, `source_os`, `utm_source`, `created_at`, `updated_at`) \
                VALUES(NULL, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)"
            val = (client_id, source_platform, utm_ad_id, source_lead_id,
                   source_ip, source_device, source_ismobile, source_os, utm_source, created_at)
            cursor.execute(sql, val)
        except Error as e:
            print(sql, val)
            print(e)
            break

        if n % batch_size == 0:
            print("iteration %s" % n)
            time.sleep(0.5)
            conn.commit()
except Error as e:
    print("error", e)
    pass
except Exception as e:
    print("Unknown error %s", e)
finally:
    # closing database connection.
    if(conn and conn.is_connected()):
        conn.commit()
        cursor.close()
        conn.close()
