import os
import json
import datagen
import argparse
from flask import render_template, session, request

from session_utils import (get_connection_status,
                           get_pg_connection_status,
                           get_backend_config,
                           get_pg_backend_config,
                           create_dataset,
                           create_pg_dataset,
                           get_backend_status,
                           get_keys_count,
                           get_index_status,
                           flushdatabase,
                           get_session_data,
                           set_session_data,
                           reset_session,
                           format_product_doc,
                           get_products,
                           refresh_prod_data,
                           get_pg_products,
                           search,
                           pgsearch,
                           pg_prod_data,
                           pg_update_prod,
                           autocomplete,
                           pg_autocomplete,
                           get_shopping_cart,
                           set_shopping_cart,
                           update_shopping_cart)

from . import home_bp

def parse_args():
    parser = argparse.ArgumentParser(description="Insert Records")
    parser.add_argument(
        "--hostname",
        type=str,
        help="Hostname",
        default="localhost",
        required=False
    )
    parser.add_argument(
        "--port",
        type=int,
        help="Starting Port",
        default=5000,
        required=False
    )
    parser.add_argument(
        "--debug",
        type=str,
        help="Debug Mode",
        default="false",
        required=False
    )
    return parser.parse_args()

args = parse_args()
hostname = args.hostname
port = args.port
debug = (args.debug=="true")

print(f"--> Starting App. Hostname: {hostname} | Port: {port} | Debug: {debug} ")

# Dataset - Get Current Config
@home_bp.route('/dbconfig', methods=['GET'])
def dbconfig_get():
    config_data = {}
    config_obj = get_backend_config()
    pg_config = get_pg_backend_config()
    if config_obj is None:
        config_data = {"host": "", "port": "", "user": "", "password": "", "pg_host" : "", "pg_port" : "", "pg_db" : "", "pg_user" : "", "pg_pass" : ""}
    else:
        config_data = {
            "host" : config_obj['R_HOST'],
            "port" : config_obj['R_PORT'],
            "user" : config_obj['R_USER'],
            "password" : config_obj['R_PASSWORD'],
            "pg_host" : pg_config['HOST'],
            "pg_port" : pg_config['PORT'],
            "pg_db" : pg_config['DB'],
            "pg_user" : pg_config['USER'],
            "pg_pass" : pg_config['PASSWORD']
        }
    
    return json.dumps(config_data)

# Dataset - Save Configuration
@home_bp.route('/saveconfig', methods=['POST'])
def saveconfig_post():
    if debug:
        print("--> Saving Configuration")
    redis_host = request.values.get('redis_host')
    redis_port = request.values.get('redis_port')
    redis_user = request.values.get('redis_user')
    redis_pass = request.values.get('redis_pass')

    conn_result = get_connection_status(redis_host, redis_port, redis_user, redis_pass)
    if conn_result == "success":
        session['redis_host'] = redis_host
        session['redis_port'] = redis_port
        session['redis_user'] = redis_user
        session['redis_pass'] = redis_pass
        return "success"
    else:
        return conn_result

@home_bp.route('/savepgconfig', methods=['POST'])
def savepgconfig_post():
    if debug:
        print("--> Saving PG Configuration")
    pg_host = request.values.get('pg_host')
    pg_port = request.values.get('pg_port')
    pg_db = request.values.get('pg_db')
    pg_user = request.values.get('pg_user')
    pg_pass = request.values.get('pg_pass')

    conn_result = get_pg_connection_status(pg_host, pg_port, pg_db, pg_user, pg_pass)
    if conn_result == "success":
        session['pg_host'] = pg_host
        session['pg_port'] = pg_port
        session['pg_db'] = pg_db
        session['pg_user'] = pg_user
        session['pg_pass'] = pg_pass
        return "success"
    else:
        return conn_result

# Dataset - Test DB Connection
@home_bp.route('/testconn', methods=['POST'])
def testconn():
    if debug:
        print("--> Testing DB Connectivity")
    redis_host = request.values.get('redis_host')
    redis_port = request.values.get('redis_port')
    redis_user = request.values.get('redis_user')
    redis_pass = request.values.get('redis_pass')

    conn_result = get_connection_status(redis_host, redis_port, redis_user, redis_pass)
    if conn_result == "success":
        session['redis_host'] = request.values.get('redis_host')
        session['redis_port'] = request.values.get('redis_port')
        session['redis_user'] = request.values.get('redis_user')
        session['redis_pass'] = request.values.get('redis_pass')
        return "success"
    else:
        return conn_result

@home_bp.route('/testpgconn', methods=['POST'])
def testpgconn():
    if debug:
        print("--> Testing DB Connectivity")
    pg_host = request.values.get('pg_host')
    pg_port = request.values.get('pg_port')
    pg_db = request.values.get('pg_db')
    pg_user = request.values.get('pg_user')
    pg_pass = request.values.get('pg_pass')

    conn_result = get_pg_connection_status(pg_host, pg_port, pg_db, pg_user, pg_pass)
    if conn_result == "success":
        session['pg_host'] = request.values.get('pg_host')
        session['pg_port'] = request.values.get('pg_port')
        session['pg_db'] = request.values.get('pg_db')
        session['pg_user'] = request.values.get('pg_user')
        session['pg_pass'] = request.values.get('pg_pass')
        return "success"
    else:
        return conn_result    

# Dataset - Flush DB
@home_bp.route('/flushdb', methods=['POST'])
def flushdb_post():
    if debug:
        print("--> Flushing DB")
    flushdatabase()
    return "success"

# Dataset - Get Keys count
@home_bp.route('/keycount', methods=['GET'])
def keycount_get():
    return json.dumps(get_keys_count())

# Dataset - Create Dataset and Indexes
@home_bp.route('/createdataset', methods=['POST'])
def createdataset_post():
    if debug:
        print("--> Creating Dataset and Indexes")
    ds_info = create_dataset()
    return json.dumps(ds_info, default=list)

@home_bp.route('/createpgdataset', methods=['POST'])
def createpgdataset_post():
    if debug:
        print("--> Creating PG Dataset")
    ds_info = create_pg_dataset(debug)
    return json.dumps(ds_info, default=list)

# Dataset - Get Indexes Status
@home_bp.route('/indexcheck', methods=['GET'])
def indexcheck_get():
    return json.dumps(get_index_status())

# Setup Page
@home_bp.route('/config')
def setup_page():
    return render_template('/setup.html', hostname=hostname, port=port, debug=debug)

# Home Page
@home_bp.route('/')
def index():
    if get_backend_status():
        if debug:
            print("--> Loading Product List Page")
        return render_template('/redis.html', hostname=hostname, port=port, debug=debug)
    else:
        print("--> Database is not ready! Loading setup page")
        return render_template('/setup.html', hostname=hostname, port=port, debug=debug)

# Session Data
@home_bp.route('/session', methods=['GET'])
def url_session():
    if debug:
        print(f"--> Getting User Session | Current session: {session.get('session_id')}")
    # 1 - Session does not exist - create new session
    current_session_id = session.get("session_id")
    current_session_data = get_session_data(current_session_id, debug)
    if current_session_data is None or len(current_session_data) == 0:
        session_id = datagen.get_session_id()
        session["session_id"] = session_id
        session_data = datagen.get_session_data(session_id)
        print(f"--> Session ID doesn't exist! Created new session ID: {session_id}")

        # Register session in Redis
        reset_session(session_id, debug)
        set_session_data(session_id, session_data)
        session_data['session_id'] = session_id
        session.modified = True
        return json.dumps(session_data)
    # 2 Session exists
    else:
        session_id = session.get("session_id")
        session_info = get_session_data(session_id, debug)
        # print(f"--> Session Info: {session_info}")
        session_info[0]['session_id'] = session_id
        if debug:
            print(f"--> Return data for session ID: {session_id}")
        return json.dumps(session_info[0])

# Get List of Products
@home_bp.route('/products', methods=['POST'])
def url_product_list():
    numprods = request.values.get('numprods')
    if debug:
        print(f"--> Getting Product List: {numprods}")

    total_docs = get_index_status()
    results = get_products(int(numprods))
    product_list = []
    for product in results:
        product_info = format_product_doc(product)
        product_list.append(product_info)
    
    return json.dumps([total_docs, product_list])

@home_bp.route('/refresh_prod_data', methods=['POST'])
def url_refresh_prod_data():
    prod_list_str = request.values.get('prod_list')
    prod_list = json.loads(prod_list_str)
    # if debug:
    #     print(f"--> Refreshing Product Data with: {len(prod_list)} products")

    results = refresh_prod_data(prod_list)
    product_list = []
    for product in results:
        product_info = format_product_doc(product)
        product_list.append(product_info)
    
    return json.dumps(product_list)

@home_bp.route('/pg_products', methods=['POST'])
def url_pg_product_list():
    numprods = request.values.get('numprods')
    if debug:
        print(f"--> Getting Postgres Product List: {numprods}")
    product_list = []
    pg_config = get_pg_backend_config()
    if pg_config is not None:
        pg_host = pg_config['HOST']
        pg_port = pg_config['PORT']
        pg_db = pg_config['DB']
        pg_user = pg_config['USER']
        pg_pass = pg_config['PASSWORD']
        conn_result = get_pg_connection_status(pg_host, pg_port, pg_db, pg_user, pg_pass)
        if conn_result == "success":
            results = get_pg_products(int(numprods), debug)
            for product in results:
                product_info = format_product_doc(product)
                product_list.append(product_info)
    total_docs = get_keys_count()
    return json.dumps([total_docs, product_list])

# Get Autocomplete
@home_bp.route('/autocomplete', methods=['POST'])
def autocomplete_function():
    val = request.values.get('val')
    total_items = 0
    try:
        response = autocomplete(val)
        result_list = []
        for item in response:
            result_list.append(item.productDisplayName)
        total_items = len(result_list)
        val_list = json.dumps(result_list)
    except Exception as ex:
        print(f"--> [autocomplete_function] ERROR GETTING AUTOCOMPLETE: {ex}")
        val_list = []   

    if debug:
        print(f"--> Getting Autocomplete List for Val: {val} with {total_items} results")
    return val_list 

@home_bp.route('/pg_autocomplete', methods=['POST'])
def pg_autocomplete_function():
    val = request.values.get('val')
    total_items = 0
    try:
        response = pg_autocomplete(val, debug)
        result_list = []
        for item in response:
            result_list.append(item['productdisplayname'])
        total_items = len(result_list)
        val_list = json.dumps(result_list)
    except Exception as ex:
        print(f"--> [pg_autocomplete_function] ERROR GETTING AUTOCOMPLETE: {ex}")
        val_list = []   
    
    if debug:
        print(f"--> Getting Autocomplete List for Val: {val} with {total_items} results")
    return val_list 

# Search Products
@home_bp.route('/search', methods=['POST'])
def url_search():
    val = request.values.get('query_string')
    if debug:
        print(f"--> Searching for: {val}")
    if val == "":
        return get_products(16)
    else:
        search_results = search('productDisplayName', val)
        response = []
        for doc in search_results:
            product_info = format_product_doc(json.loads(doc.json))
            response.append(product_info)
        return json.dumps(response)

# Shopping Cart
@home_bp.route('/shopping_cart', methods=['GET'])
def shopping_cart_function():
    try:
        session_id = session.get("session_id")
        # if debug:
        #     print(f"--> Getting Shopping Cart for session: {session_id}")
        shopping_cart = get_shopping_cart(session_id, debug)
        if shopping_cart is None:
            return []
        else:
            return json.dumps(shopping_cart)

    except Exception as ex:
        error = f"Error Getting Shopping Cart: {ex}"
        print(f"[shopping_cart_function] {error}")
        return error

@home_bp.route('/add_to', methods=['POST'])
def addto_function():
    op_type = request.values.get('type')
    prod_id = request.values.get('prod_id')
    quantity = request.values.get('quantity')
    session_id = session.get("session_id")
    return_data = []
    if debug:
        print(f"--> Add To: {op_type} | Prod ID: {prod_id} | Quantity: {quantity}")

    if op_type == 'cart':
         return_data = set_shopping_cart(prod_id, quantity, session_id, debug)

    return json.dumps(return_data)

@home_bp.route('/shopping_cart_update', methods=['POST'])
def shopping_cart_update_function():
    operation = request.values.get('operation')
    prod_id = request.values.get('prod_id')
    session_id = session.get("session_id")

    return_data = update_shopping_cart(prod_id, operation, session_id)
    if debug:
        print(f"--> Updating shopping cart: operation: {operation} | return data: {return_data}")
    return json.dumps(return_data)

# Postgres Admin Page
@home_bp.route('/postgres')
def postgres_index():
    if get_backend_status():
        if debug:
            print("--> Loading Product List Page")
        return render_template('/postgres.html', hostname=hostname, port=port, debug=debug)
    else:
        print("--> Database is not ready! Loading setup page")
        return render_template('/setup.html', hostname=hostname, port=port, debug=debug)

@home_bp.route('/pgsearch', methods=['POST'])
def url_pgsearch():
    val = request.values.get('query_string')
    if debug:
        print(f"--> Searching PG for: {val}")
    if val == "":
        return get_pg_products(10, debug)
    else:
        results = pgsearch(val, debug)
        product_list = []
        for product in results:
            product_info = format_product_doc(product)
            product_list.append(product_info)
        return json.dumps(product_list)

@home_bp.route('/pg_prod_data', methods=['POST'])
def url_pg_prod_data():
    prod_id = request.values.get('prod_id')
    if debug:
        print(f"--> Getting PG Product details: {prod_id}")
    results = pg_prod_data(prod_id, debug)
    product_info = format_product_doc(results)
    return json.dumps(product_info)

@home_bp.route('/pg_update_prod', methods=['POST'])
def url_pg_update_prod():
    prod_id = request.values.get('prod_id')
    productdisplayname = request.values.get('productdisplayname')
    discountedprice = request.values.get('discountedprice')
    price = request.values.get('price')
    rating = request.values.get('rating')
    articletype = request.values.get('articletype')
    productdescriptors = request.values.get('productdescriptors')
    inventorycount = request.values.get('inventorycount')
    if debug:
        print(f"--> Updating PG Product details: {prod_id}")
    results = pg_update_prod(prod_id, productdisplayname, discountedprice, price, rating, articletype, productdescriptors, inventorycount)
    return results

