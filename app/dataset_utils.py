import os
import json
import random
from decimal import Decimal
from configparser import ConfigParser
from flask import session

import psycopg2
from psycopg2 import sql
from psycopg2.extras import Json
from psycopg2 import OperationalError
from psycopg2.extras import execute_values
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

import redis
from redis.cache import CacheConfig
from redis.commands.json.path import Path
from redis.commands.search.query import Query
from redis.commands.search.index_definition import IndexDefinition, IndexType
from redis.commands.search.field import NumericField, TagField, TextField, VectorField

r = None

# Config Screen Utils
# ------------------------------------------------------------------------------
def get_backend_config():
    config_doc = None
    try:
        config_obj = ConfigParser()
        config_obj.read("./config.ini")
        config_doc = {
            "R_HOST" : config_obj['REDIS_INFO']['host'],
            "R_PORT" : config_obj['REDIS_INFO']['port'],
            "R_USER" : config_obj['REDIS_INFO']['user'],
            "R_PASSWORD" : config_obj['REDIS_INFO']['password']
        }
        
    except Exception as ex:
        print(f"--> [get_backend_config] ERROR: {ex}")
    
    return config_doc

def get_redis_client():
    global r
    conn_info = get_backend_config()
    if conn_info is not None:
        R_HOST = conn_info['R_HOST']
        R_PORT = conn_info['R_PORT']
        R_USER = conn_info['R_USER']
        R_PASSWORD = conn_info['R_PASSWORD']
        if r is None:
            try:
                r = redis.Redis(host=R_HOST, 
                                port=R_PORT, 
                                username=R_USER, 
                                password=R_PASSWORD, 
                                decode_responses=True,
                                protocol=3,
                                cache_config=CacheConfig())
                print("--> Connection to Redis successful")
            except Exception as ex:
                print(f"--> Error getting Redis Client: {ex}")
                r = None
        return r

def get_backend_status():
    global r
    isReady = False
    if r is None:
        try:
            r = get_redis_client()
        except Exception as ex:
            print(f"get_backend_status: Error getting the Redis client: {ex}")
        return isReady
    else:
        # Check if Postgres is available and has data
        if get_pg_key_count() > 0:
            isReady = True
    return isReady

def get_key_count(key_type):
    global r
    if r is None:
        r = get_redis_client()    
    try:
        counter = 0
        prefix = f"{key_type}:*"
        for key in r.scan_iter(match=prefix):
                counter += 1
        return counter
    except:
        return 0

def test_connection(host, port, user, password):
    global r
    try:
        r = redis.Redis(host=host, port=port, username=user, password=password, decode_responses=True)

        if r.ping():
            # uncomment this line for docker environments
            set_backend_config(host, port, user, password)
            result = create_product_index()
            return "success"
    except Exception as ex:
        return f"Connection failure: {ex}"

def set_backend_config(host, port, user, password):
    result = "FAIL"
    try:
        config = ConfigParser()
        config.read('./config.ini')

        config['REDIS_INFO'] = {
            'host': host,
            'port': port,
            'user' : user,
            'password' : password
        }

        with open('./config.ini', 'w') as configfile:
            config.write(configfile)
        
        result = "SUCCESS"
    except Exception as ex:
        result = f"FAIL TO CREATE CONFIG FILE: {ex}"

    return result

def flushdb():
    global r
    if r is None:
        r = get_redis_client()
    r.flushall()

def load_dataset(ds_type):
    if ds_type == 'product':
        ds_file = './static/dataset/product_ds.json'
    
    with open(ds_file) as f:
        dataset = json.load(f)
    print(f"--> Loaded dataset for {ds_type} with {len(dataset)} records.")
    return dataset

def create_product_index():
    global r
    if r is None:
        r = get_redis_client()    
    result = "FAILED"
    schema = (
        TextField("$.productDisplayName", as_name="productDisplayName"),
        TextField("$.articleType", as_name="articleType"),
        TextField("$.articleNumber", as_name="articleNumber"),
        TextField("$.brandName", as_name="brandName"),
        TextField("$.variantName", as_name="variantName"),
        TextField("$.ageGroup", as_name="ageGroup"),
        TextField("$.gender", as_name="gender"),
        TextField("$.fashionType", as_name="fashionType"),
        TextField("$.season", as_name="season"),
        TextField("$.year", as_name="year"),
        TextField("$.masterCategory", as_name="masterCategory"),
        TextField("$.subCategory", as_name="subCategory"),
        TextField("$.displayCategories", as_name="displayCategories"),
        TextField("$.flag_fragile", as_name="flag_fragile"),
        TextField("$.flag_tryandbuy", as_name="flag_tryandbuy"),
        TextField("$.flag_return", as_name="flag_return"),
        TextField("$.flag_exchange", as_name="flag_exchange"),
        TextField("$.flag_pickup", as_name="flag_pickup"),
        TextField("$.baseColour", as_name="baseColour"),
        TextField("$.productDescriptors", as_name="productDescriptors"),
        TextField("$.image_48x64", as_name="image_48x64"),
        TextField("$.image_1080X1440", as_name="image_1080X1440"),
        TextField("$.image_150X200", as_name="image_150X200"),
        TextField("$.image_360X480", as_name="image_360X480"),
        NumericField("$.id", as_name="id"),
        NumericField("$.price", as_name="price"),
        NumericField("$.discountedPrice", as_name="discountedPrice"),
        NumericField("$.catalogAddDate", as_name="catalogAddDate"),
        NumericField("$.vat", as_name="vat"),
        NumericField("$.rating", as_name="rating"),
        NumericField("$.discount_pct", as_name="discount_pct"),
    )
    try:
        r.ft("idx:product").dropindex()
    except:
        print("--> Product index doesn't exist; creating it")
    try:
        definition = IndexDefinition(prefix=["product:"], index_type=IndexType.JSON)
        result = r.ft("idx:product").create_index(fields=schema, definition=definition)
    except Exception as ex:
        result = f"FAILED to create index: {ex}"
    return result

def get_index_status():
    global r
    if r is None:
        r = get_redis_client()    
    try:
        info = r.ft('idx:product').info()
        return info['num_docs']
    except Exception as ex:
        print("Index does not exist!")
        return 0

def insert_product_documents(dataset):
    global r
    if r is None:
        r = get_redis_client()
    try:
        pipeline = r.pipeline()
        for document in dataset:
            redis_key = f"product:{document['id']}"
            pipeline.json().set(redis_key, "$", document)
            res = pipeline.execute()

    except Exception as e:
        result = f"---> [insert_product_documents] Save Dataset FAILED: {e}"
    return len(res)

def save_redis_dataset():
    dataset = load_dataset("product")
    index_result = create_product_index()
    insert_prod_result = insert_product_documents(dataset)
    if insert_prod_result > 0:
        return {"result" : "success", "index" : index_result, "keys" : insert_prod_result}
    else:
        return {"result" : "fail", "index" : index_result, "keys" : insert_prod_result}

# ------------------------------------------------------------------------------
# Redis Functions
# ------------------------------------------------------------------------------

def get_redis_product_id_list(amount):
    global r
    if r is None:
        r = get_redis_client()
    product_id_list = []
    try:
        # Get 10 times more results than requested so we can return a true 'random' set
        query = Query('*').return_field("$.id", as_field="id").paging(0, amount*10)
        query_response = r.ft("idx:product").search(query)
        for doc in query_response['results']:
            product_id_list.append(doc['id'])
        return random.sample(product_id_list, amount)

    except Exception as ex:
        print(f"--> [get_redis_product_id_list] ERROR getting list of Product IDs: {ex}")
        return product_id_list

# ------------------------------------------------------------------------------
# Postgres Functions
# ------------------------------------------------------------------------------

def test_pg_connection(host, port, db, user, password):
    conn = None
    cursor = None
    status = "success"
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            database="postgres", # Connect to a default database to create a new one
            user=user,
            password=password
        )

        cursor = conn.cursor()
        sql_statement = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
        cursor.execute(sql_statement)

        # if successful, save configuration to file
        set_pg_backend_config(host, port, db, user, password)

    except psycopg2.Error as e:
        status = e
        print(f"Error connecting to database: {e}")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        return status

def get_pg_backend_config():
    try:
        config_obj = ConfigParser()
        config_obj.read("./config.ini")
        config_doc = {
            "HOST" : config_obj['PG_INFO']['host'],
            "PORT" : config_obj['PG_INFO']['port'],
            "DB" : config_obj['PG_INFO']['db'],
            "USER" : config_obj['PG_INFO']['user'],
            "PASSWORD" : config_obj['PG_INFO']['password']
        }        
    except:
        config_doc = None
    
    return config_doc

def set_pg_backend_config(host, port, db, user, password):
    result = "FAIL"
    try:
        config = ConfigParser()
        config.read('./config.ini')

        config['PG_INFO'] = {
            'host': host,
            'port': port,
            'db' : db,
            'user' : user,
            'password' : password
        }

        with open('./config.ini', 'w') as configfile:
            config.write(configfile)
        
        result = "SUCCESS"
    except Exception as ex:
        result = f"FAIL TO CREATE CONFIG FILE: {ex}"

    return result    

def pg_check_db_and_table():
    dbExists = False
    tableExists = False

    pg_info = get_pg_backend_config()
    if pg_info is not None:
        PG_HOST = pg_info['HOST']
        PG_PORT = pg_info['PORT']
        PG_DB = pg_info['DB']
        PG_USER = pg_info['USER']
        PG_PASSWORD = pg_info['PASSWORD']

        try:
            # Check if Database exists by connecting as Postgres
            print(f"--> Checking if DB {PG_DB} exists")
            conn = psycopg2.connect(dbname="postgres", host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASSWORD)
            cursor = conn.cursor()
            cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{PG_DB}'")
            dbExists = cursor.fetchone() is not None
            print(f"--> DB Exists: {dbExists}")
            cursor.close()
            conn.close()
        except OperationalError:
            # This might happen if the 'postgres' database itself is unreachable
            print(f"Could not connect to default database to check for '{PG_DB}'.")

        try:
            # Check if table exists
            print(f"--> Checking if Table ecommerce exists")
            conn = psycopg2.connect(dbname=PG_DB, host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASSWORD)
            cursor = conn.cursor()
            # cursor.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'ecommerce')")
            cursor.execute("SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'ecommerce'")
            result = cursor.fetchone()
            tableExists = result is not None
            print(f"--> Table Exists: {result} | {tableExists}")
            cursor.close()
            conn.close()
        except OperationalError as e:
            # This might happen if the 'postgres' database itself is unreachable
            print(f"Could not connect to database or table does not exist")

    return dbExists, tableExists

def pg_check_rdi_user():
    userExists = False
    pg_info = get_pg_backend_config()
    if pg_info is not None:
        PG_HOST = pg_info['HOST']
        PG_PORT = pg_info['PORT']
        PG_DB = pg_info['DB']
        PG_USER = pg_info['USER']
        PG_PASSWORD = pg_info['PASSWORD']

        try:
            print(f"--> Checking if DB {PG_DB} exists")
            conn = psycopg2.connect(dbname="postgres", host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASSWORD)
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM pg_roles WHERE rolname='redisuser';")
            result = cursor.fetchone()
            userExists = result is not None
            print(f"--> User Exists: {result} | {userExists}")
            cursor.close()
            conn.close()
        except OperationalError:
            print("Could not connect to default database to check for user.")

    return userExists

def pg_create_database():
    result = "FAIL"
    pg_info = get_pg_backend_config()
    if pg_info is not None:
        PG_HOST = pg_info['HOST']
        PG_PORT = pg_info['PORT']
        PG_DB = pg_info['DB']
        PG_USER = pg_info['USER']
        PG_PASSWORD = pg_info['PASSWORD']

        try:
            conn = psycopg2.connect(dbname="postgres", host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASSWORD)
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            conn.autocommit = True
            cursor = conn.cursor()
            cursor.execute(sql.SQL('CREATE DATABASE {};').format(sql.Identifier(PG_DB)))
            if not pg_check_rdi_user():
                
                cursor.execute(sql.SQL('CREATE USER redisuser REPLICATION LOGIN'))
                
                sql_statement = """ALTER USER redisuser WITH PASSWORD %(password)s"""
                cursor.execute(sql_statement, {"password": "welcome1"})
                
                cursor.execute(sql.SQL('ALTER USER redisuser WITH SUPERUSER'))

                cursor.execute(sql.SQL('GRANT ALL PRIVILEGES ON DATABASE {} TO redisuser').format(sql.Identifier(PG_DB)))
                
                conn.commit()

                print(f"--> User Creation: success")
                
            result = "success"

        except psycopg2.Error as e:
            result = e
            print(f"Error creating database: {e}")

        finally:
            # Close the cursor and connection
            if cursor:
                cursor.close()
            if conn:
                conn.close()
            return result

def pg_create_table():
    result = "FAIL"
    pg_info = get_pg_backend_config()
    if pg_info is not None:
        PG_HOST = pg_info['HOST']
        PG_PORT = pg_info['PORT']
        PG_DB = pg_info['DB']
        PG_USER = pg_info['USER']
        PG_PASSWORD = pg_info['PASSWORD']

        try:
            conn = psycopg2.connect(dbname=PG_DB, host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASSWORD)
            cursor = conn.cursor()

            create_table_sql = """
                CREATE TABLE ecommerce (
                    id int NOT NULL,
                    price numeric NULL,
                    discountedprice numeric NULL,
                    articlenumber varchar NULL,
                    productdisplayname varchar NULL,
                    variantname varchar NULL,
                    catalogadddate int NULL,
                    brandname varchar NULL,
                    agegroup varchar NULL,
                    gender varchar NULL,
                    basecolour varchar NULL,
                    fashiontype varchar NULL,
                    season varchar NULL,
                    "year" varchar NULL,
                    vat numeric NULL,
                    rating int NULL,
                    displaycategories varchar NULL,
                    image_48x64 varchar NULL,
                    image_1080x1440 varchar NULL,
                    image_150x200 varchar NULL,
                    image_360x480 varchar NULL,
                    mastercategory varchar NULL,
                    subcategory varchar NULL,
                    articletype varchar NULL,
                    flag_fragile varchar NULL,
                    flag_tryandbuy varchar NULL,
                    flag_return varchar NULL,
                    flag_exchange varchar NULL,
                    flag_pickup varchar NULL,
                    productdescriptors text NULL,
                    discount_pct int NULL,
                    CONSTRAINT ecommerce_pk PRIMARY KEY (id)
                );
            """

            # Execute the SQL statement
            cursor.execute(create_table_sql)

            # Commit the changes to the database
            conn.commit()
            result = "success"

        except psycopg2.Error as e:
            result = e
            print(f"Error creating table: {e}")
            if conn:
                conn.rollback()

        finally:
            # Close the cursor and connection
            if cursor:
                cursor.close()
            if conn:
                conn.close()
            return result

def insert_pg_documents(dataset):
    i = 0
    pg_info = get_pg_backend_config()
    if pg_info is not None:
        PG_HOST = pg_info['HOST']
        PG_PORT = pg_info['PORT']
        PG_DB = pg_info['DB']
        PG_USER = pg_info['USER']
        PG_PASSWORD = pg_info['PASSWORD']    

        try:
            conn = psycopg2.connect(dbname=PG_DB, host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASSWORD)
            cursor = conn.cursor()
            list_of_values = []
            for document in dataset:
                list_of_values.append((
                    document['id'], 
                    document['price'], 
                    document['discountedPrice'], 
                    document['articleNumber'], 
                    document['productDisplayName'], 
                    document['variantName'], 
                    document['catalogAddDate'], 
                    document['brandName'], 
                    document['ageGroup'], 
                    document['gender'],
                    document['baseColour'], 
                    document['fashionType'], 
                    document['season'], 
                    document['year'], 
                    document['vat'], 
                    document['rating'], 
                    document['displayCategories'], 
                    document['image_48x64'], 
                    document['image_1080X1440'], 
                    document['image_150X200'], 
                    document['image_360X480'], 
                    document['masterCategory'], 
                    document['subCategory'], 
                    document['articleType'], 
                    document['flag_fragile'], 
                    document['flag_tryandbuy'], 
                    document['flag_return'],  
                    document['flag_exchange'], 
                    document['flag_pickup'], 
                    document['productDescriptors'], 
                    document['discount_pct']
                ))

            sql_statement = f"""
                INSERT INTO ecommerce (id, price, discountedprice, articlenumber, productdisplayname, variantname, 
                    catalogadddate, brandname, agegroup, gender, basecolour, fashiontype, season, year, vat, rating, 
                    displaycategories, image_48x64, image_1080x1440, image_150x200, image_360x480, mastercategory, 
                    subcategory, articletype, flag_fragile, flag_tryandbuy, flag_return, flag_exchange, flag_pickup, 
                    productdescriptors, discount_pct) 
                VALUES %s
            """
            
            execute_values(cursor, sql_statement, list_of_values)
            i = len(list_of_values)
            conn.commit()
        
        except Exception as e:
            conn.rollback()
            print(f"Could save records to table: {e}")

        finally:
            cursor.close()
            conn.close()

    return i

def save_pg_dataset():
    dataset = load_dataset("product")
    db_result = ""
    table_result = ""

    # Check if DB and Table exist; otherwise create them.
    dbExists, tableExists = pg_check_db_and_table()
    if not dbExists:
        db_result = pg_create_database()
        if db_result == "success":
            dbExists = True
        print(f"--> DB creation: {db_result}")
    
    if not tableExists:
        table_result = pg_create_table()
        if table_result == "success":
            tableExists = True
        print(f"--> Table creation: {table_result}")        

    if dbExists and tableExists:
        insert_prod_result = insert_pg_documents(dataset)

        if insert_prod_result > 0:
            return {"result" : "success", "keys" : insert_prod_result}
        else:
            return {"result" : "fail", "keys" : insert_prod_result}
    else:
        return {"result" : "fail", "keys" : 0}

def get_pg_key_count():
    count = 0
    pg_info = get_pg_backend_config()
    if pg_info is not None:
        PG_HOST = pg_info['HOST']
        PG_PORT = pg_info['PORT']
        PG_DB = pg_info['DB']
        PG_USER = pg_info['USER']
        PG_PASSWORD = pg_info['PASSWORD']

        try:
            conn = psycopg2.connect(dbname=PG_DB, host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASSWORD)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM ecommerce")
            result = cursor.fetchone()
            print(f"--> Postgres record count: {result[0]}")
            cursor.close()
            conn.close()
            return result[0]
        except OperationalError as e:
            # This might happen if the 'postgres' database itself is unreachable
            print(f"--> Postgres is not ready or reachable.")
            return 0

def get_pg_products(amount):
    pg_info = get_pg_backend_config()
    results = []
    if pg_info is not None:
        PG_HOST = pg_info['HOST']
        PG_PORT = pg_info['PORT']
        PG_DB = pg_info['DB']
        PG_USER = pg_info['USER']
        PG_PASSWORD = pg_info['PASSWORD']

        try:
            conn = psycopg2.connect(dbname=PG_DB, host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASSWORD)
            cursor = conn.cursor()
            cursor.execute(f"SELECT * FROM ecommerce ORDER BY RANDOM() LIMIT {amount}")
            results = cursor.fetchall()
            print(f"--> Retrieved Postgres products: {len(results)}")

            # Get column names from cursor description
            columns = [col.name for col in cursor.description]

            # Convert rows to a list of dictionaries
            json_results = []
            for row in results:
                row_dict = {}
                for i, column_name in enumerate(columns):
                    if isinstance(row[i], Decimal):
                        row_dict[column_name] = float(row[i])
                    else:
                        row_dict[column_name] = row[i]
                json_results.append(row_dict)

            cursor.close()
            conn.close()
            return json_results
        except OperationalError:
            # This might happen if the 'postgres' database itself is unreachable
            print(f"Could not connect to default database to check for '{PG_DB}'.")
            return results

def pgsearch(term):
    pg_info = get_pg_backend_config()
    results = []
    if pg_info is not None:
        PG_HOST = pg_info['HOST']
        PG_PORT = pg_info['PORT']
        PG_DB = pg_info['DB']
        PG_USER = pg_info['USER']
        PG_PASSWORD = pg_info['PASSWORD']

        try:
            conn = psycopg2.connect(dbname=PG_DB, host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASSWORD)
            cursor = conn.cursor()
            sql_statement = f"SELECT * FROM ecommerce where lower(productdisplayname) LIKE '{term.lower()}%%' LIMIT 10"
            cursor.execute(sql_statement)
            results = cursor.fetchall()
            print(f"--> Retrieved Postgres products: {len(results)}")

            # Get column names from cursor description
            columns = [col.name for col in cursor.description]

            # Convert rows to a list of dictionaries
            json_results = []
            for row in results:
                row_dict = {}
                for i, column_name in enumerate(columns):
                    if isinstance(row[i], Decimal):
                        row_dict[column_name] = float(row[i])
                    else:
                        row_dict[column_name] = row[i]
                json_results.append(row_dict)

            cursor.close()
            conn.close()
            return json_results
        except OperationalError:
            # This might happen if the 'postgres' database itself is unreachable
            print(f"Failed to search for term '{term}'.")
            return results 

def pg_autocomplete(term):
    return pgsearch(term)

def pg_prod_data(prod_id):
    pg_info = get_pg_backend_config()
    results = []
    if pg_info is not None:
        PG_HOST = pg_info['HOST']
        PG_PORT = pg_info['PORT']
        PG_DB = pg_info['DB']
        PG_USER = pg_info['USER']
        PG_PASSWORD = pg_info['PASSWORD']

        try:
            conn = psycopg2.connect(dbname=PG_DB, host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASSWORD)
            cursor = conn.cursor()
            cursor.execute(f"SELECT * FROM ecommerce where id = {prod_id}")
            results = cursor.fetchone()
            
            # Get column names from cursor description
            columns = [col.name for col in cursor.description]

            # Convert rows to a list of dictionaries
            row_dict = {}
            for i, column_name in enumerate(columns):
                if isinstance(results[i], Decimal):
                    row_dict[column_name] = float(results[i])
                else:
                    row_dict[column_name] = results[i]
            json_results = row_dict
            
            # print(f"--> Retrieved Postgres product details: {json_results}")
            cursor.close()
            conn.close()
            return json_results
        except OperationalError:
            # This might happen if the 'postgres' database itself is unreachable
            print(f"Failed to search for prod_id '{prod_id}'.")
            return results

def pg_update_prod(prod_id, productdisplayname, discountedprice, price, rating, articletype, productdescriptors):
    pg_info = get_pg_backend_config()
    results = []
    if pg_info is not None:
        PG_HOST = pg_info['HOST']
        PG_PORT = pg_info['PORT']
        PG_DB = pg_info['DB']
        PG_USER = pg_info['USER']
        PG_PASSWORD = pg_info['PASSWORD']

        try:
            conn = psycopg2.connect(dbname=PG_DB, host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASSWORD)
            cursor = conn.cursor()
            sql_statement = f"""
                UPDATE ecommerce set
                productdisplayname = %s,
                discountedprice = %s, 
                price = %s, 
                rating = %s, 
                articletype = %s, 
                productdescriptors = %s
                WHERE id = %s
            """
            sql_data = (productdisplayname, discountedprice, price, rating, articletype, productdescriptors, prod_id)
            cursor.execute(sql_statement, sql_data)
            conn.commit()
            cursor.close()
            conn.close()
            return "success"
        except OperationalError:
            # This might happen if the 'postgres' database itself is unreachable
            print(f"Failed to search for prod_id '{prod_id}'.")
            return results    