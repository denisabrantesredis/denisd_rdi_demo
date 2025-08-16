import json
import time
import random
import datagen
from flask import session
from decimal import Decimal
from configparser import ConfigParser

import psycopg2
from psycopg2 import sql
from psycopg2 import OperationalError
from psycopg2.extras import execute_values
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

import redis
from redis.cache import CacheConfig
from redis.commands.search.query import Query
from redis.commands.search.index_definition import IndexDefinition, IndexType
from redis.commands.search.field import NumericField, TextField

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
                print(f"--> [get_redis_client] Error getting Redis Client: {ex}")
                r = None
        return r

def get_backend_status():
    global r
    isReady = False
    if r is None:
        try:
            r = get_redis_client()
        except Exception as ex:
            print(f"---> [get_backend_status] Error getting the Redis client: {ex}")
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

def get_keys_count():
    redis_count = get_key_count("product")
    pg_count = get_pg_key_count()
    response = {"redis" : redis_count, "pg" : pg_count}
    return response

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
        return f"--> [test_connection] Connection failure: {ex}"

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
        result = f"--> [set_backend_config] FAIL TO CREATE CONFIG FILE: {ex}"

    return result

def flushdb():
    global r
    if r is None:
        r = get_redis_client()
    r.flushall()

def flushdatabase():
    flushdb()

def load_dataset(ds_type):
    if ds_type == 'product':
        ds_file = './static/dataset/product_dataset.json'
    
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
        NumericField("$.inventoryCount", as_name="inventoryCount"),
    )
    try:
        r.ft("idx:product").dropindex()
    except:
        print("--> Product index doesn't exist; creating it")
    try:
        definition = IndexDefinition(prefix=["product:"], index_type=IndexType.JSON)
        result = r.ft("idx:product").create_index(fields=schema, definition=definition)
    except Exception as ex:
        result = f"--> [create_product_index] FAILED to create index: {ex}"
    return result

def get_index_status():
    global r
    if r is None:
        r = get_redis_client()    
    try:
        info = r.ft('idx:product').info()
        return info['num_docs']
    except Exception as ex:
        print(f"--> [get_index_status] Index does not exist! {ex}")
        return 0

def insert_product_document(document):
    global r
    if r is None:
        r = get_redis_client()    
    result = "FAILED"
    try:
        pipeline = r.pipeline()
        redis_key = f"product:{document['id']}"
        pipeline.json().set(redis_key, "$", document)
        res = pipeline.execute()
        result = f"{redis_key} record inserted successfully"
    except Exception as e:
        result = f"FAILED with error: {e}"
    return result

def insert_product_documents(dataset):
    i = 0
    for product in dataset:
        insert_result = insert_product_document(product)
        if not insert_result.startswith("FAILED"):
            i = i + 1
    return i

def save_redis_dataset():
    dataset = load_dataset("product")
    index_result = create_product_index()
    insert_prod_result = insert_product_documents(dataset)
    if insert_prod_result > 0:
        return {"result" : "success", "index" : index_result, "keys" : insert_prod_result}
    else:
        return {"result" : "fail", "index" : index_result, "keys" : insert_prod_result}

def create_dataset():
    product_status = save_redis_dataset()
    return_msg = {
        "prod_result" : product_status['result'], 
        "prod_index" : product_status['index'],
        "prod_keys" : product_status['keys'],
    }
    return return_msg

def get_connection_status(host, port, user, password):
    return test_connection(host, port, user, password)

# ------------------------------------------------------------------------------
# Redis Functions
# ------------------------------------------------------------------------------

# Search Utils
def search(attribute, value):
    try:
        r = get_redis_client()
        query = Query(f'@{attribute}:{value}')
        query_response = r.ft("idx:product").search(query).docs
        return query_response
    except Exception as ex:
        print(f"---> [search] Failed to run search: {ex}")
    return None    

def run_query(query_prefix, index_name):
    try:
        r = get_redis_client()
        responses = "FAILED TO RUN QUERY"
        start = time.perf_counter()
        query = (Query(query_prefix))
        try:
            responses = r.ft(index_name).search(query).docs
        except Exception as ex:
            responses = [f"ERROR RUNNING QUERY: {ex}"]    
        end = time.perf_counter()
        elapsed = end - start
        print(f'Query Time: {elapsed:.6f} seconds')
        return responses
    except Exception as ex:
        print(f"--> [run_query] Failed to run query: {ex}")
    return None

def autocomplete(val):
    try:
        r = get_redis_client()
        search_words = val.split(' ')
        for word in search_words:
            if len(word) < 2:
                val = val + 'a'
        query = Query(f"@productDisplayName:{val}*").return_fields('id', 'productDisplayName').scorer('BM25').with_scores()
        query_response = r.ft("idx:product").search(query).docs
        return query_response    
    except Exception as ex:
        print(f"--> [autocomplete] Failed to run autocomplete: {ex}")
        return None

# Session, Product & User Management Utils
def reset_session(session_id, debug):
    if debug:
        print("--> Resetting the session")
    config_data = {}
    config_obj = get_backend_config()
    if config_obj is None:
        config_data = {"host": "", "port": "", "user": "", "password": "", "gcp_api_key": "" }
    else:
        config_data = {
            "host" : config_obj['R_HOST'],
            "port" : config_obj['R_PORT'],
            "user" : config_obj['R_USER'],
            "password" : config_obj['R_PASSWORD'],
        }
    REDIS_URL = f"redis://default:{config_data['password']}@{config_data['host']}:{config_data['port']}"

    session.clear()
    session['_permanent'] = True
    session['redis_host'] = config_data['host']
    session['redis_port'] = config_data['port']
    session['redis_user'] = config_data['user']
    session['redis_pass'] = config_data['password']
    session['session_id'] = session_id
    session.modified = True

def get_session_data(session_id, debug):
    global r
    if r is None:
        r = get_redis_client()
    try:
        key = f"session:{session_id}"
        session_data = r.json().get(key, "$")
        if debug:
            print(f"--> [get_session_data] Returning Session Data: {session_data}")
        return session_data
    except Exception as ex:
        print(f"--> [get_session_data] Failed to get session data: {ex}")
        return None

def set_session_data(session_id, session_data):
    global r
    if r is None:
        r = get_redis_client()
    try:
        key = f"session:{session_id}"
        return r.json().set(key, "$", session_data)
    except Exception as ex:
        print(f"---> [set_session_data] Failed to set session data: {ex}")
    return None

def format_product_doc(product_info):
    # Formats the image path fields, ratings fields, etc.
    stars_counter = 0
    rating = product_info['rating']
    result = int(rating / 2)
    remainder = rating % 2

    rating_html = ""
    for i in range(result):
        rating_html += "<i class='fa fa-star'></i>"
        stars_counter = stars_counter + 1
    if remainder > 0:
        rating_html += "<i class='fa fa-star-half-o'></i>"
        stars_counter = stars_counter + 1

    for i in range (5 - stars_counter):
        rating_html += "<i class='fa fa-star-o'></i>"

    product_info["rating_html"] = rating_html
    
    return product_info    

def get_redis_product_id_list(amount):
    global r
    if r is None:
        r = get_redis_client()
    product_id_list = []
    try:
        # Get 10 times more results than requested so we can return a true 'random' set
        query = Query('*').return_field("$.id", as_field="id").paging(0, amount*10)
        query_response = r.ft("idx:product").search(query)
        # print(query_response)
        for doc in query_response.docs:
            product_id_list.append(doc['id'])
        return random.sample(product_id_list, amount)

    except Exception as ex:
        print(f"--> [get_redis_product_id_list] ERROR getting list of Product IDs: {ex}")
        return product_id_list

def get_products(amount):
    global r
    if r is None:
        r = get_redis_client()    
    try:
        total_docs = get_index_status()
        product_list = []

        # Get random products
        if total_docs > 0:
            # Get a list of Product IDs (they're not sequential)
            product_ids = get_redis_product_id_list(amount)
            for product_id in product_ids:
                product_info = r.json().get(product_id)
                product_list.append(product_info)      

        return product_list
    except Exception as ex:
        print(f"---> [get_products] Failed to get session data: {ex}")
    return None

def refresh_prod_data(prod_list):
    product_list = []
    global r
    if r is None:
        r = get_redis_client()    
    try:
        for product_id in prod_list:
            key = f"product:{product_id}"
            product_info = r.json().get(key)
            product_list.append(product_info)      

        return product_list
    except Exception as ex:
        print(f"---> [refresh_prod_data] Failed to get session data: {ex}")
    
    return product_list

def save_full_list(key, items, session_id):
    global r
    if r is None:
        r = get_redis_client()    
    try:
        list_items = []
        for item in items:
            item['session_id'] = session_id
            timestamp = datagen.get_actual_timestamp("float")
            this_key = f"{key}:{timestamp}"
            r.json().set(this_key, "$", item)
            list_items.append(timestamp)
        r.json().set(key, "$", list_items)
    except Exception as ex:
        print(f"--> [save_full_list] Failed to saving full list: {ex}")
    return None

# Shoping Cart Utils
def get_shopping_cart(session_id, debug):
    shopping_cart_items = []
    shopping_cart_list = []
    hasShoppingCart = False
    try:
        global r
        if r is None:
            r = get_redis_client()
        key = f"session:{session_id}"
        shopping_cart_list = r.json().get(key, "$.shopping_cart")
        # if debug:
        #     print(f"--> [get_shopping_cart] shopping_cart_list: {shopping_cart_list}")
        if shopping_cart_list is not None and len(shopping_cart_list) > 0:
            for item in shopping_cart_list[0]:
                for key in item.keys():
                    if key == 'prod_id':
                        hasShoppingCart = True
        if hasShoppingCart:
            for item in shopping_cart_list[0]:
                prod_id = item['prod_id']
                quantity = item['quantity']
                prod_key = f"product:{prod_id}"
                prod_info = r.json().get(prod_key, "$")
                if prod_info:
                    prod_details = prod_info[0]
                    price = prod_details["discountedPrice"]
                    total_cost = price * int(quantity)            
                    prod_data = {
                        "id" : prod_details["id"],
                        "productDisplayName" : prod_details["productDisplayName"],
                        "discountedPrice" : price,
                        "discount_pct" : prod_details["discount_pct"],
                        "amount" : int(quantity),
                        "total_cost" : total_cost,
                        "added_ts" : 1234,
                        "image_48x64" : prod_details["image_48x64"],
                        "session_id" : session_id
                    }
                    shopping_cart_items.append(prod_data)
        # if debug:
        #     print(f"--> [get_shopping_cart] Returning shopping cart: {len(shopping_cart_items)} items")
    except Exception as ex:
        print(f"--> [get_shopping_cart] ERROR getting shopping cart: {ex}")
    return shopping_cart_items

def set_shopping_cart(prod_id, quantity, session_id, debug):
    shopping_cart = []
    global r
    if r is None:
        r = get_redis_client()    
    try:               
        key = f"session:{session_id}"
        shopping_cart_list = r.json().get(key, "$.shopping_cart")
        prod_data = {"prod_id" : prod_id, "quantity" : int(quantity)}
        if shopping_cart_list:
            r.json().arrappend(key, "$.shopping_cart", prod_data)
        else:
            r.json().set(key, "$.shopping_cart", [prod_data])

        shopping_cart = get_shopping_cart(session_id, debug)
    except Exception as ex:
        print(f"--> [set_shopping_cart] Failed to save shopping cart: {ex}")
        shopping_cart = None
    
    return shopping_cart

def update_shopping_cart(prod_id, operation, session_id):
    global r
    if r is None:
        r = get_redis_client()
    try:
        key = f"session:{session_id}"
        shopping_cart_list = r.json().get(key, "$.shopping_cart")[0]

        response_data = {"prod_id" : prod_id, "amount" : 0, "cart_total" : 0, "cart_total_items" : 0}
        cart_total = 0
        cart_total_items = 0
        shopping_cart_updated_list = []
        
        # Go through the list to find the product that is being updated
        for list_item in shopping_cart_list:            
            prod_id_l = list_item["prod_id"]
            quantity = int(list_item["quantity"])

            if int(prod_id_l) == int(prod_id):
                # if changing amount
                if operation == 'minus' or operation == 'plus':
                    if operation == 'minus':
                        quantity -= 1
                        if quantity == 0:
                            quantity = 1
                    if operation == 'plus':
                        quantity += 1

                    list_item["quantity"] = quantity
                    response_data['amount'] = quantity

            # Get Prod details to calculate total price
            prod_key = f"product:{prod_id_l}"
            prod_info = r.json().get(prod_key, "$")
            if prod_info:
                prod_details = prod_info[0]
                prod_cost = prod_details['discountedPrice'] * quantity

                # Add all other products to the list
                if int(prod_id_l) != int(prod_id):
                    cart_total += prod_cost
                    cart_total_items += 1
                    shopping_cart_updated_list.append(list_item)
                else:
                    if operation != 'del':
                        cart_total += prod_cost
                        cart_total_items += 1                        
                        shopping_cart_updated_list.append(list_item)

        # Update key with new quantities
        r.json().set(key, "$.shopping_cart", shopping_cart_updated_list)

        response_data["cart_total"] = round(cart_total,2)
        response_data["cart_total_items"] = cart_total_items

        return response_data
    except Exception as ex:
        print(f"--> [update_shopping_cart] Failed to update shopping cart: {ex}")
        return None

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
        print(f"[test_pg_connection] Error connecting to Postgres: {e}")

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
        print(f"--> [set_pg_backend_config] Error saving config file:{ex}")

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
                    inventorycount int NULL,
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
                    document['discount_pct'],
                    document['inventoryCount']
                ))

            sql_statement = f"""
                INSERT INTO ecommerce (id, price, discountedprice, articlenumber, productdisplayname, variantname, 
                    catalogadddate, brandname, agegroup, gender, basecolour, fashiontype, season, year, vat, rating, 
                    displaycategories, image_48x64, image_1080x1440, image_150x200, image_360x480, mastercategory, 
                    subcategory, articletype, flag_fragile, flag_tryandbuy, flag_return, flag_exchange, flag_pickup, 
                    productdescriptors, discount_pct, inventorycount) 
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

def save_pg_dataset(debug):
    dataset = load_dataset("product")
    db_result = ""
    table_result = ""

    # Check if DB and Table exist; otherwise create them.
    dbExists, tableExists = pg_check_db_and_table()
    if not dbExists:
        db_result = pg_create_database()
        if db_result == "success":
            dbExists = True
        if debug:
            print(f"--> DB creation: {db_result}")
    
    if not tableExists:
        table_result = pg_create_table()
        if table_result == "success":
            tableExists = True
        if debug:
            print(f"--> Table creation: {table_result}")        

    if dbExists and tableExists:
        insert_prod_result = insert_pg_documents(dataset)

        if insert_prod_result > 0:
            return {"result" : "success", "keys" : insert_prod_result}
        else:
            return {"result" : "fail", "keys" : insert_prod_result}
    else:
        return {"result" : "fail", "keys" : 0}

def create_pg_dataset(debug):
    return save_pg_dataset(debug)

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

def get_pg_products(amount, debug):
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
            if debug:
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
            print(f"---> [get_pg_products] Could not connect to default database to check for '{PG_DB}'.")
            return results

def pgsearch(term, debug):
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
            if debug:
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
            print(f"---> [pgsearch] Failed to search for term '{term}'.")
            return results 

def pg_autocomplete(term, debug):
    return pgsearch(term, debug)

def pg_prod_data(prod_id, debug):
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
            
            if debug:
                print(f"--> Retrieved Postgres product details: {json_results}")
            cursor.close()
            conn.close()
            return json_results
        except OperationalError:
            # This might happen if the 'postgres' database itself is unreachable
            print(f"---> [pg_prod_data] Failed to search for prod_id '{prod_id}'.")
            return results

def pg_update_prod(prod_id, productdisplayname, discountedprice, price, rating, articletype, productdescriptors, inventorycount):
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
                productdescriptors = %s,
                inventorycount = %s
                WHERE id = %s
            """
            sql_data = (productdisplayname, discountedprice, price, rating, articletype, productdescriptors, inventorycount, prod_id)
            cursor.execute(sql_statement, sql_data)
            conn.commit()
            cursor.close()
            conn.close()
            return "success"
        except OperationalError:
            # This might happen if the 'postgres' database itself is unreachable
            print(f"---> [pg_update_prod] Failed to search for prod_id '{prod_id}'.")
            return results    

def get_pg_connection_status(host, port, db, user, password):
    return test_pg_connection(host, port, db, user, password)

def format_pg_product_doc(product_info):
    # Formats the image path fields, ratings fields, etc.
    
    stars_counter = 0
    rating = product_info['rating']
    result = int(rating / 2)
    remainder = rating % 2

    rating_html = ""
    for i in range(result):
        rating_html += "<i class='fa fa-star'></i>"
        stars_counter = stars_counter + 1
    if remainder > 0:
        rating_html += "<i class='fa fa-star-half-o'></i>"
        stars_counter = stars_counter + 1

    for i in range (5 - stars_counter):
        rating_html += "<i class='fa fa-star-o'></i>"

    product_info["rating_html"] = rating_html
    
    return product_info    

