import json
import uuid
import faker
import random
from random import randrange
from collections import OrderedDict
from datetime import datetime, timedelta
from faker.providers import misc, lorem, internet, company, file, currency, geo, job

fake = faker.Faker()
fake.add_provider(misc)
fake.add_provider(lorem)
fake.add_provider(internet)
fake.add_provider(company)
fake.add_provider(file)
fake.add_provider(currency)
fake.add_provider(geo)
fake.add_provider(job)

def get_fake_id(length):
	length = int(length)
	return fake.password(length=length, special_chars=False, digits=True, upper_case=True, lower_case=False)

def get_fake_uuid():
    return fake.uuid4()

def get_fake_text(range):
	max_chars = int(range)
	return fake.text(max_nb_chars=max_chars)

def get_fake_number(range):
	try:
		range_list = range.split(",", 1)
		range_list[0] = int(range_list[0])
		range_list[1] = int(range_list[1])
	except:
		range_list = [0, 1000]
	return random.randint(range_list[0], range_list[1])

def get_fake_float(range):
	try:
		range_list = range.split(",", 1)
		range_list[0] = float(range_list[0])
		range_list[1] = float(range_list[1])
	except:
		range_list = [0.0, 1000.0]	
	return round(random.uniform(range_list[0], range_list[1]),2)

def get_fake_name(name_type):
	if name_type == "first":
		return fake.first_name()
	else:
		if name_type == "last":
			return fake.last_name()
		else:
			return fake.first_name() + " " + fake.last_name()

def get_fake_timestamp(days, difference):
	timestamp = ""
	if difference == "-":
		past_date = datetime.now() - timedelta(days=days)
		delta = datetime.now() - past_date
		int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
		random_second = randrange(int_delta)
		timestamp = datetime.now() - timedelta(seconds =random_second)		
	else:
		future_date = datetime.now() + timedelta(days=days)
		delta = future_date - datetime.now()
		int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
		random_second = randrange(int_delta)
		timestamp = datetime.now() + timedelta(seconds =random_second)
	return timestamp.strftime('%Y-%m-%dT%H:%M:%SZ')

def get_fake_dateofbirth(start, end):
	datetime_format = '%Y-%m-%d'
	init_date = datetime.strptime(start, datetime_format)
	end_date = datetime.strptime(end, datetime_format)
	ptime = end_date - init_date
	total_days = ptime.days

	random_day = randrange(total_days)

	dob = init_date + timedelta(days=random_day)
	
	return dob.strftime('%Y-%m-%d')

def get_fake_phone():
	return fake.phone_number()

def get_fake_email():
	return fake.ascii_email()

def get_fake_job_title():
	return fake.job().title()

def get_fake_location(locationtype):
	if locationtype == "street":
		return fake.street_address()
	if locationtype == "city":
		return fake.city()
	if locationtype == "state":
		return fake.state()
	if locationtype == "zipcode":
		return fake.postcode()
	if locationtype == "country":
		return fake.country()

def get_fake_boolean():
	return fake.boolean(chance_of_getting_true=50)

def get_fake_coordinates():
	return fake.local_latlng(country_code='US', coords_only=True)

def get_fake_company():
	return fake.company()

def get_fake_url():
	return fake.url()

def get_fake_ip():
	return fake.ipv4()

def get_fake_file_path(depth):
	return fake.file_path(depth=depth)

def get_fake_list_of_strings(num_items):
	list_of_values = fake.words(nb=num_items)
	string_list = ','.join(list_of_values)
	return string_list

def get_fake_list_of_ints(range_start, range_end):
	num_items = range_end - range_start
	number_list = random.sample(range(range_start, range_end), num_items * 10)
	mydict = {}
	for i in range(0, len(number_list), 2):
		mydict[number_list[i]] = 0.01

	fake_list = fake.random_elements(elements=OrderedDict(mydict), unique=False, length=num_items)
	return fake_list

def get_fake_option(option_list):
	return random.choice(option_list)


# Data Generators for E-Commerce User Profile

def get_personal_data(user_id):
	full_name = get_fake_name("full")
	first_name = full_name.split(" ")[0] 
	last_name = full_name.split(" ")[1]

	return {
		"id" : user_id,
		"first_name" : first_name,
		"last_name" : last_name,
		"full_name" : full_name,
		"email" : f"{first_name.lower()}.{last_name.lower()}@gmail.com", 
		"date_of_birth" : get_fake_dateofbirth("1970-01-01", "2006-11-01"),
		"gender" : get_fake_option(['m', 'f']),
		"customer_since_ts" : get_fake_timestamp(3000, "-"),
		"language" : "en",
		"profile_picture" : f"{user_id}.png"
	}

def get_session_id():
	return f"{str(uuid.uuid4())}"

def get_session_data(session_id):
	return {
		"user_id" : session_id,
		"last_login_ts" : get_fake_timestamp(30, "-"),
		"current_login_ts" : 0,
		"session_created_ts" : datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
	}

def get_products_viewed(product_list):
	products_viewed = []
	num_products_viewed = get_fake_number("1,5")

	for i in range(num_products_viewed):
		this_product = get_fake_option(product_list)
		products_viewed.append({
			"id" : this_product["id"],
			"productDisplayName" : this_product["productDisplayName"],
			"discountedPrice" : this_product["discountedPrice"],
			"discount_pct" : this_product["discount_pct"],
			"viewed_ts" : get_fake_timestamp(29, "-"),
			"image_48x64" : this_product["image_48x64"]
		})
	return products_viewed

def get_shopping_cart(product_list):
	cart_items = []
	num_cart_items = get_fake_number("1,4")

	for i in range(num_cart_items):
		this_product = get_fake_option(product_list)
		amount = get_fake_number("1,3")
		total = this_product["discountedPrice"] * amount
		cart_items.append({
			"id" : this_product["id"],
			"productDisplayName" : this_product["productDisplayName"],
			"discountedPrice" : round(this_product["discountedPrice"],2),
			"discount_pct" : this_product["discount_pct"],
			"amount" : amount,
			"total_cost" : round(total,2),
			"added_ts" : get_fake_timestamp(29, "-"),
			"image_48x64" : this_product["image_48x64"]
		})

	return cart_items

def get_wishlist(product_list):
	wishlist_items = []
	num_wishlist_items = get_fake_number("1,2")

	for i in range(num_wishlist_items):
		this_product = get_fake_option(product_list)
		wishlist_items.append({
			"id" : this_product["id"],
			"productDisplayName" : this_product["productDisplayName"],
			"discountedPrice" : round(this_product["discountedPrice"],2),
			"discount_pct" : this_product["discount_pct"],
			"added_ts" : get_fake_timestamp(29, "-"),
			"image_48x64" : this_product["image_48x64"]
		})
	return wishlist_items

def get_previous_orders(product_list):
	order_items = []
	num_order_items = get_fake_number("1,3")

	for i in range(num_order_items):
		this_product = get_fake_option(product_list)
		amount = get_fake_number("1,3")
		total = this_product["discountedPrice"] * amount
		payment_status = get_fake_option(['Complete', 'Complete', 'Complete', 'Complete', 'Complete', 'Failed', 'Cancelled'])
		delivery_status = 'Pending'
		delivery_ts = '0'
		if payment_status == 'Complete':
			order_status = get_fake_option(['Complete', 'Complete', 'Complete', 'Processing'])
			delivery_status = get_fake_option(['Delivered', 'Delivered', 'Delivered', 'In Progress', 'In Progress', 'Delayed'])
		else: 
			order_status = payment_status
			delivery_status = 'Cancelled'
		if delivery_status == 'Delivered':
			delivery_ts = get_fake_timestamp(14, "-")
		order_items.append({
			"id" : this_product["id"],
			"productDisplayName" : this_product["productDisplayName"],
			"discountedPrice" : round(this_product["discountedPrice"],2),
			"discount_pct" : this_product["discount_pct"],
			"amount" : amount,
			"total_cost" : round(total,2),
			"image_48x64" : this_product["image_48x64"],
			"order_ts" : get_fake_timestamp(28, "-"),
			"order_status" : order_status,
			"payment_status" : payment_status,
			"delivery_status" : delivery_status,
			"delivery_ts" : delivery_ts,
			"return_limit_ts" : get_fake_timestamp(30, "+")
			
		})
	return order_items

def get_search_history(product_list):
	search_history = []
	used_words = []
	num_search_items = get_fake_number("2,4")
	should_break = False

	for i in range(num_search_items * 10):
		this_product = get_fake_option(product_list)
		bag_of_words = this_product["productDisplayName"].split(" ")
		for word in bag_of_words:
			if len(word) > 3:
				if word not in used_words:
					used_words.append(word)
					search_history.append({
						"term" : word,
						"search_ts" : get_fake_timestamp(14, "-")
					})
					if len(search_history) >= num_search_items:
						should_break = True
						break
		if should_break:
			break

	return search_history

def get_actual_timestamp(type="str"):
	if type == "float":
		return str(datetime.now().timestamp())
	else:
		return datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')