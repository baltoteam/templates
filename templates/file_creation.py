import argparse
import csv
import editdistance
import os
import sys
import pandas as pd
import xlrd
from os.path import dirname, abspath

sys.path.append(os.path.join(dirname(dirname(abspath(__file__))), "python templates"))

from collections import defaultdict
from datetime import datetime, timedelta
from math import floor
from tempfile import TemporaryDirectory

from templates.run_list import export_run_list, get_route_names, update_run_name_with_area
from utils.tools import excel_to_csv
from utils.parameters import DROPBOX_FOLDER, TIME_SLOTS_AM, WAREHOUSES, FROM_WW_HEADER
from utils.tools import find_date


def read_data(filepath, sheet_name):
	if filepath.endswith(".xlsx") or filepath.endswith(".xls"):
		df = pd.read_excel(filepath, sheet_name)
	else:
		with open(filepath, 'r') as f:
			try:
				df = pd.read_csv(f, sep=",", skip_blank_lines=True)
			except:
				with open(filepath, encoding="utf-8") as f:
					df = pd.read_csv(f, sep=",", skip_blank_lines=True)
	# Remove blank rows and blank columns
	df = df.dropna(axis=0, how='all')
	# Save only deliveries, remove warehouse stops

	# Add missing columns
	for col in FROM_WW_HEADER:
		if col not in df.columns:
			df[col] = ["" for i in range(len(df))]
	return df[df["Type"] == "delivery"].reset_index()


def fix_duplicate_names(df, name_column):
	names = []
	name_number = defaultdict(int)
	for name in df[name_column]:
		if name in name_number.keys():
			number = name_number[name]
			full_name = f"{name} {number}"
		else:
			full_name = name
		names.append(full_name)
		name_number[name] += 1
	return names


def get_routed_RouteVehicle(df):
	route_names = []
	for ind in df.index:
		run_number = df["Vehicle"][ind].split(" ")[-1]
		route_names.append(f"Balto {run_number}")
	return route_names


def get_time_slot(df):
	time_slots = []
	for ind in df.index:
		client = df["client"][ind]
		if client == "HelloFresh":
			time_window_start = df["hf tws"][ind]
			time_window_end = df["hf twe"][ind]
		else:
			time_window_start = df["Time Window Start"][ind]
			time_window_end = df["Time Window End"][ind]
		try:
			time_window_start = (datetime(2000, 1, 1) + timedelta(hours=time_window_start * 24)).strftime("%H:%M")
			time_window_end = (datetime(2000, 1, 1) + timedelta(hours=time_window_end * 24)).strftime("%H:%M")
		except TypeError:
			pass
		time_slots.append(f"{time_window_start} - {time_window_end}")
	return time_slots


def get_time_slot2(df, time_slot):
	time_slots = []
	for ind in df.index:
		client = df["client"][ind]
		if client == "HelloFresh":
			time_window_start = df["hf tws"][ind]
			time_window_end = df["hf twe"][ind]
			try:
				time_window_start = (datetime(2000, 1, 1) + timedelta(hours=time_window_start * 24)).strftime("%H:%M")
				time_window_end = (datetime(2000, 1, 1) + timedelta(hours=time_window_end * 24)).strftime("%H:%M")
			except: #TypeError:
				pass
			time_slots.append(f"{time_window_start} - {time_window_end}")
		else:
			time_slots.append(time_slot)
	return time_slots


def split_df(df):
	runs = set()
	df_per_run = {}
	for ind in df.index:
		run_number = df["Route"][ind]
		runs.add(run_number)
	for run_number in runs:
		df_per_run[run_number] = df[df["Route"] == run_number]
	return df_per_run


def fix_phone_numbers(df):
	phone_numbers = []
	for ind in df.index:
		number = str(df["phone"][ind]).replace(" ", "").split(".")[0]
		# number = f"{number_int:10d}"
		if not number.startswith("0"):
			if len(number) == 9:
				number = "0" + number
			else:
				print(f"Warning: Phone number {number} is not valid.")
		phone_numbers.append(f'="{number}"')
	return phone_numbers


def concatenate_instructions(df):
	instructions = []
	for ind in df.index:
		notes = df["Notes"][ind]
		# company = df["company"][ind]
		# instruction = df["Instructions"][ind]
		# instructions.append(f"{notes} - {company} - {instruction}")
		instructions.append(notes)
	return instructions


def find_warehouse_address(name):
    score = 100000
    o = None
    warehouse = WAREHOUSES.keys()
    for w in warehouse:
        if w in name:
            return WAREHOUSES[w]
        if editdistance.eval(name, w) < score:
            score = editdistance.eval(name, w)
            o = w
    return WAREHOUSES[o]


def add_temperature_lines_to_detrack(output_df, date, warehouse_address):
	run_dict = output_df.groupby("Route")
	df_list = []
	for run in run_dict.groups:
		df = run_dict.get_group(run)
		first_index = df.index[0]
		last_index = df.index[-1]
		middle_index = int((first_index + last_index) / 2)

		# Create temperature lines
		temp1 = pd.DataFrame([["Warehouse", "WarehouseTEMPERATURE01",
							   warehouse_address, "Australia", "", "Take a picture of temperature gun",
							   date, run, "", 0, "", f"{run}TEMPERATURE01", "", "", ""]],
							   columns=df.columns)

		drop_number_temp2 = int(df["Stop Number"][middle_index]) + 0.9
		temp2 = pd.DataFrame([["", f"{run}TEMPERATURE02",
							   df["Address"][middle_index], "Australia", "",
							   "Take a picture of temperature gun",
							   date, run, "", drop_number_temp2,
							   "", f"{run}TEMPERATURE02", "", "", ""]],
							   columns=df.columns)

		drop_number_temp3 = int(df["Stop Number"][last_index]) + 0.9
		temp3 = pd.DataFrame([["", f"{run}TEMPERATURE03",
							   df["Address"][last_index], "Australia", "",
							   "Take a picture of temperature gun",
							   date, run, "", drop_number_temp3,
							   "", f"{run}TEMPERATURE03", "", "", ""]],
							   columns=df.columns)

		df_list.append(pd.concat([temp1, df.loc[first_index:middle_index - 1],
					   temp2, df.loc[middle_index:], temp3]))

	return pd.concat(df_list)


def add_temperature_lines_to_hds_detrack(output_df, date, warehouse_address):
	run_dict = output_df.groupby("Vehicle")
	df_list = []
	for run in run_dict.groups:
		df = run_dict.get_group(run)
		first_index = df.index[0]
		last_index = df.index[-1]
		middle_index = int((first_index + last_index) / 2)

		# Create temperature lines
		temp1 = pd.DataFrame([[run, date, 0, "", "", "delivery", warehouse_address, "", "",
							   1, "", "", "", "WarehouseTEMPERATURE01", "", f"{run}TEMPERATURE01", "", "", 
							   "Take a picture of temperature gun", "", ""]], columns=df.columns)


		drop_number_temp2 = int(df["Order Number"][middle_index]) + 0.9
		# temp2 = pd.DataFrame([["", f"{run}TEMPERATURE02",
		# 					   df["Address"][middle_index], "Australia", "",
		# 					   "Take a picture of temperature gun",
		# 					   date, run, "", drop_number_temp2,
		# 					   "", f"{run}TEMPERATURE02", "", "", ""]],
		# 					   columns=df.columns)

		temp2 = pd.DataFrame([[run, date, drop_number_temp2, "", "", "delivery", df["Address"][middle_index], "", "",
							   1, "", "", "", "WarehouseTEMPERATURE02", "", f"{run}TEMPERATURE02", "", "", 
							   "Take a picture of temperature gun", "", ""]], columns=df.columns)

		drop_number_temp3 = int(df["Order Number"][last_index]) + 0.9
		# temp3 = pd.DataFrame([["", f"{run}TEMPERATURE03",
		# 					   df["Address"][last_index], "Australia", "",
		# 					   "Take a picture of temperature gun",
		# 					   date, run, "", drop_number_temp3,
		# 					   "", f"{run}TEMPERATURE03", "", "", ""]],
		# 					   columns=df.columns)
		temp3 = pd.DataFrame([[run, date, drop_number_temp3, "", "", "delivery", df["Address"][last_index], "", "",
							   1, "", "", "", "WarehouseTEMPERATURE03", "", f"{run}TEMPERATURE03", "", "", 
							   "Take a picture of temperature gun", "", ""]], columns=df.columns)

		df_list.append(pd.concat([temp1, df.loc[first_index:middle_index],
					   temp2, df.loc[middle_index + 1:], temp3]))

	return pd.concat(df_list)


def eta_bounds(df, hds):
	lower = []
	upper = []
	if hds:
		time_format = "%H:%M"
	else:
		time_format = "%I:%M %p"
	for time in df["Current Schedule"]:
		dt = datetime.strptime(time, time_format)
		lower_bound = (dt - timedelta(minutes=30)).strftime(time_format)
		upper_bound = (dt + timedelta(hours=1, minutes=30)).strftime(time_format)

		lower.append(lower_bound)
		upper.append(upper_bound)
	return lower, upper


def get_do(input_df, output_df, routes, stop_numbers):
	dos = []
	for ind in input_df.index:
		if input_df["client"][ind] == "HelloFresh":
			dos.append(input_df["d.o"][ind])
		else:
			# D.Os for customers other than HF are:
			# Ba_{run name}#{drop number}
			run_name = routes[ind]
			drop_number = stop_numbers[ind]
			if drop_number - floor(drop_number) == 0.0:
				drop_number = int(drop_number)
			dos.append(f"Ba_{run_name}#{drop_number}")
	return dos


def make_driver_manifest(df, year, week, weekday, state, is_am, reg, hds, route_area):
	output_df = pd.DataFrame(columns=["Customer", "Address", "Phone", "Instructions",
									  "Date", "Route", "Drop", "Company", "Time slot"])
	# Assigning right columns
	output_df["Customer"] = fix_duplicate_names(df, "Name")
	output_df["Address"] = df["Address"]
	output_df["Phone"] = fix_phone_numbers(df)
	output_df["Instructions"] = concatenate_instructions(df)
	output_df["Date"] = [find_date(year, week, weekday) for i in range(len(output_df))]
	output_df["Route"] = get_route_names(df, weekday, state, is_am, reg, hds)
	output_df["Drop"] = df["Step Number"]
	output_df["Company"] = df["client"]
	# "HelloFresh" timeslot or 08:00 - 18:00
	output_df["Time slot"] = get_time_slot(df)

	output_dict = split_df(output_df)
	return output_df, output_dict



def dispatch_manager_file(filename, year, week, weekday, state, is_am, reg, hds, warehouse_address, dispatch_manager, sync_time):
	filepath = os.path.join(DROPBOX_FOLDER, f"Weeks {year}", f"Week {week:02d}", state, "03 - From WW", filename)
	workbook = xlrd.open_workbook(filepath)
	sheet_name = "Mixed"
	if "Mixed" not in workbook.sheet_names():
		sheet_name = "Sheet0"
	df = read_data(filepath, sheet_name)
	if is_am:
		date = find_date(year, week, weekday) - timedelta(days=1)
		if sync_time == "":
			sync_time = datetime(2000, 1, 1, 21, 0).time()
	else:
		date = find_date(year, week, weekday)

	balto_list = []
	hds_list = []
	run_number = 0
	full_state = state
	if reg:
		full_state += " REG"
	if is_am:
		full_state += " AM"
	for run in df.groupby("Vehicle").groups:
		if hds:
			run_number += 1
			run_name = str(run)
			run_id = f"{weekday[:3]}_{full_state}_{run_name}"
		else:
			run_number = int(run.split(" ")[-1])
			run_name = f"{weekday[:3]} {full_state} {run_number:02d}"
			run_id = f"{weekday[:3]}_{full_state}_{run_number:02d}"

		balto_list.append(["", run_name, warehouse_address, "Australia", "",
						 "Please check the driver and van and report missing boxes in the drop notes - 1 - Photo of van - 2 Photo of Temperature - 3 Photo of driver",
						 date, f"Dispatch_{weekday[:3]}_{full_state}".replace(" ", "_"),
						 dispatch_manager, 0.01 * run_number, "", f"Dispatch_{run_name}".replace(" ", "_"), "", "", sync_time])
		hds_list.append([f"Dispatch_{weekday[:3]}_{full_state}".replace(" ", "_"), date, 0.01 * run_number, "", dispatch_manager, "delivery",
					   warehouse_address, "", "", 1, "", "", "", run_name, "", f"Dispatch_{run_id}".replace(" ", "_"),
					   "", "", "Please check the driver and van and report missing boxes in the drop notes - 1 - Photo of van - 2 Photo of Temperature - 3 Photo of driver",
					   "", sync_time])

	balto_df = pd.DataFrame(balto_list, columns=["Customer ID", "Customer", "Address", "Country",
									 			 "Phone", "Instructions", "Date", "Route", "Assign to",
									 			 "Stop Number", "Company", "D.O.", "Email", "Time slot", "Sync time"])

	hds_df = pd.DataFrame(hds_list, columns=["Vehicle", "Date", "Order Number", "Current Schedule", "Assign to",
								   			 "Type", "Address", "Latitude", "Longitude", "Load item quantity",
								   			 "client code", "client field 1", "client name", "customer",
								   			 "email", "hds id", "order id", "phone", "Notes", "Time slot", "Sync time"])
	balto_path = os.path.join(DROPBOX_FOLDER, f"Weeks {year}", f"Week {week:02d}", state, "07 - Dispatch manager", f"dispatch manager - {full_state} - {weekday}.xlsx")
	hds_path = os.path.join(DROPBOX_FOLDER, f"Weeks {year}", f"Week {week:02d}", state, "07 - Dispatch manager", f"dispatch manager HDS - {full_state} - {weekday}.xlsx")

	balto_df.to_excel(balto_path, index=False)
	hds_df.to_excel(hds_path, index=False)



def make_detrack_file(df, year, week, weekday, state, is_am, warehouse_address, reg, hds, sync_time, route_area):
	output_df = pd.DataFrame(columns=["Customer ID", "Customer", "Address", "Country",
									  "Phone", "Instructions", "Date", "Route", "Assign to",
									  "Stop Number", "Company", "D.O.", "Email", "Time slot", "Sync time"])

	# Assigning right columns
	output_df["Customer ID"] = df["id"]
	output_df["Customer"] = fix_duplicate_names(df, "Name")
	output_df["Address"] = df["Address"]
	# output_df["Country"] = df["country"]
	output_df["Phone"] = fix_phone_numbers(df)
	output_df["Instructions"] = concatenate_instructions(df)
	output_df["Date"] = [find_date(year, week, weekday) for i in range(len(output_df))]
	output_df["Route"] = update_run_name_with_area(weekday, state, is_am, reg, get_route_names(df, weekday, state, is_am, reg, hds), route_area)
	output_df["Stop Number"] = df["Step Number"]
	output_df["Company"] = df["client"]
	output_df["D.O."] = get_do(df, output_df, output_df["Route"], output_df["Stop Number"])
	output_df["Email"] = df["email"]

	# "HelloFresh" timeslot or 08:00 - 18:00
	output_df["Time slot"] = get_time_slot(df)
	if sync_time != "":
		output_df["Sync time"] = [datetime.strptime(sync_time, "%H:%M").time() for i in range(len(output_df))]
	output_df = add_temperature_lines_to_detrack(output_df, find_date(year, week, weekday), warehouse_address)
	return output_df


def make_hds_detrack_file(df, year, week, weekday, state, is_am, warehouse_address, reg, hds, sync_time, route_area):
	output_df = pd.DataFrame(columns=["Vehicle", "Date", "Order Number", "Current Schedule", "Assign to",
									  "Type", "Address", "Latitude", "Longitude", "Load item quantity",
									  "client code", "client field 1", "client name", "customer",
									  "email", "hds id", "order id", "phone", "Notes", "Time slot", "Sync time"])
	output_df["Vehicle"] = get_route_names(df, weekday, state, is_am, reg, hds)
	output_df["Date"] = [find_date(year, week, weekday) for i in range(len(output_df))]
	output_df["Order Number"] = df["Step Number"]
	output_df["Current Schedule"] = df["Current Schedule"]
	output_df["Type"] = df["Type"]
	output_df["Address"] = df["Address"]
	output_df["Latitude"] = df["Latitude"]
	output_df["Longitude"] = df["Longitude"]
	if sync_time != "":
		output_df["Sync time"] = [datetime.strptime(sync_time, "%H:%M").time() for i in range(len(output_df))]
	if hds:
		output_df["customer"] = fix_duplicate_names(df, "customer")
	else:
		output_df["customer"] = fix_duplicate_names(df, "Name")
	output_df["email"] = df["email"]
	output_df["phone"] = df["phone"]
	output_df["Time slot"] = get_time_slot(df)
	output_df["Notes"] = concatenate_instructions(df)

	if hds:
		output_df["Load item quantity"] = df["Load item quantity"]
		output_df["client field 1"] = df["client field 1"]
		output_df["client name"] = df["client name"]
		output_df["hds id"] = df["hds id"]
		output_df["order id"] = df["order id"]

	else:
		output_df["client name"] = df["client"]		
		output_df["hds id"] = get_do(df, output_df, output_df["Vehicle"], output_df["Order Number"])
	output_df["client code"] = output_df["client name"]
	output_df = add_temperature_lines_to_hds_detrack(output_df, find_date(year, week, weekday), warehouse_address)
	return output_df



def make_routed_file(df, year, week, weekday, state, is_am, hds, route_area):
	output_df = pd.DataFrame(columns=["RouteVehicle", "RouteDate", "RouteStep", "Address",
									  "Service Time", "Load", "Name", "Time Window Start",
									  "Time Window End", "Instructions", "id", "Phone", "Client",
									  "HF TWS", "HF TWE", "D.O", "Latitude", "Longitude"])
	# Assigning right columns
	if not hds:
		output_df["RouteVehicle"] = get_routed_RouteVehicle(df)
	else:
		output_df["RouteVehicle"] = df["Vehicle"]
	# output_df["RouteDate"] = [find_date(year, week, weekday) for i in range(len(output_df))]
	output_df["RouteDate"] = df["Date"]
	output_df["RouteStep"] = df["Step Number"]
	output_df["Address"] = df["Address"]
	if is_am:
		output_df["Service Time"] = [3] * len(output_df)
	else:
		output_df["Service Time"] = [4] * len(output_df)
	output_df["Load"] = df["Load load"]
	output_df["Name"] = fix_duplicate_names(df, "Name")

	# The original Time Window Start -  Time Window End from WorkWave (not the HF one)
	output_df["Time Window Start"] = df["Time Window Start"]
	output_df["Time Window End"] = df["Time Window End"]
	output_df["Instructions"] = concatenate_instructions(df)
	output_df["id"] = df["id"]
	output_df["Phone"] = df["phone"]
	output_df["Client"] = df["client"]
	output_df["HF TWS"] = df["hf tws"]
	output_df["HF TWE"] = df["hf twe"]
	output_df["D.O"] = df["d.o"]
	output_df["Latitude"] = df["Latitude"]
	output_df["Longitude"] = df["Longitude"]
	return output_df


def make_customer_etas(df, year, week, weekday, state, is_am, reg, hds, route_area):
	customer_dict = df.groupby("client")
	customer_etas = {}
	for customer in customer_dict.groups:
		customer_df = customer_dict.get_group(customer)
		if customer == "HelloFresh":
			output_df = pd.DataFrame(columns=["Route", "Stop", "ID",
											  "Customer", "ETA"])

			output_df["Route"] = get_route_names(customer_df, weekday, state, is_am, reg, hds)
			output_df["Stop"] = list(customer_df["Step Number"])
			output_df["ID"] = list(customer_df["id"])
			output_df["Customer"] = list(customer_df["Name"])
			output_df["ETA"] = list(customer_df["Current Schedule"])

		else:
			output_df = pd.DataFrame(columns=["Vehicle", "Step Number", "Name",
											  "Address", "Instructions", "Phone",
											  "Time Window Start", "Time Window End",
											  "Client"])

			# Assigning right columns
			output_df["Vehicle"] = get_route_names(customer_df, weekday, state, is_am, reg, hds)
			output_df["Step Number"] = list(customer_df["Step Number"])
			output_df["Name"] = list(customer_df["Name"])
			output_df["Address"] = list(customer_df["Address"])
			output_df["Instructions"] = concatenate_instructions(customer_df)
			output_df["Phone"] = list(customer_df["phone"])
			output_df["Time Window Start"] = eta_bounds(customer_df, hds)[0]
			output_df["Time Window End"] = eta_bounds(customer_df, hds)[1]
			output_df["Client"] = list(customer_df["client"])

		customer_etas[customer] = output_df

	return customer_etas


def address_dict(from_hf_file, year, week):
	from_hf_file_path = os.path.join(DROPBOX_FOLDER, f"Weeks {year}", f"Week {week:02d}", "01 From Customer", "HelloFresh", from_hf_file)
	from_hf = pd.read_excel(from_hf_file_path)
	address_dict = defaultdict(lambda: defaultdict())
	for ind in from_hf.index:
		customer = from_hf["Customer"][ind]
		street = from_hf["Street"][ind]
		postcode = from_hf["Postcode"][ind]
		city = from_hf["City"][ind]
		address_dict[customer]["Street"] = street
		address_dict[customer]["Postcode"] = postcode
		address_dict[customer]["City"] = city

	return address_dict


def make_invoicing_for_HF(from_ww_file, from_hf_file, year, week, weekday, state, is_am, reg):
	addresses = address_dict(from_hf_file, year, week)
	from_ww_file_path = os.path.join(DROPBOX_FOLDER, f"Weeks {year}", f"Week {week:02d}", state, "03 - From WW", from_ww_file)
	df = pd.read_excel(from_ww_file_path, sheet_names="Sheet0")

	df = df[df["Type"] == "delivery"]
	df = df[df["client"] == "HelloFresh"]
	output_df = pd.DataFrame(columns=["product", "delivery_region", "customer",
									  "customer_id", "street", "postcode", "city",
									  "phone", "delivery_comment", "delivery_date",
									  "order_nr", "delivery_time", "id_subscription",
									  "box_id", "Delivery Day", "Palette", "Drop",
									  "Couriers", "Load", "Address"])

	output_df["product"] = df["type"]
	output_df["delivery_region"] = ["Sydney"] * len(df)
	output_df["customer"] = df["Name"]
	output_df["customer_id"] = df["id"]
	output_df["phone"] = df["phone"]
	output_df["delivery_comment"] = df["Notes"]
	output_df["delivery_date"] = [find_date(year, week, weekday) for i in range(len(output_df))]
	output_df["delivery_time"] = [f"{df['hf tws'][ind]} - {df['hf twe'][ind]}" for ind in df.index]
	output_df["id_subscription"] = df["id"]
	output_df["box_id"] = df["id"]
	output_df["Delivery Day"] = [weekday for i in range(len(df))]
	output_df["Palette"] = get_route_names(df, weekday, state, is_am, reg, False)
	output_df["Drop"] = df["Step Number"]
	output_df["Couriers"] = ["Balto" for i in range(len(df))]
	output_df["Load"] = df["Load load"]
	output_df["Address"] = df["Address"]

	street, postcode, city = [], [], []
	for name in output_df["customer"]:
		street.append(addresses[name]["Street"])
		postcode.append(addresses[name]["Postcode"])
		city.append(addresses[name]["City"])

	output_df["street"] = street
	output_df["postcode"] = postcode
	output_df["city"] = city

	filename = f"invoicing - {state} - {weekday}"
	if reg:
		filename += " REG"
	if is_am:
		filename += " AM"
	output_df.to_excel(f"../Operations & Tech/Weeks {year}/Week {week:02d}/{state}/03 - From WW/Invoicing/{filename}.xlsx", index=False)


def export_data(driver_manifest, driver_manifest_dict, detrack, hds_detrack, routed, customer_etas, state, weekday, week, year, is_am, reg, hds):
	if is_am:
		day = f"{weekday} AM"
	else:
		day = weekday
	if reg:
		full_state = f"{state} REG"
	else:
		full_state = state
	if hds:
		full_state += " HDS"
	driver_manifest_path = os.path.join(DROPBOX_FOLDER, f"Weeks {year}", f"Week {week:02d}", state, "04 - Driver Manifest", f"driver manifest - {full_state} - {day}.xlsx")

	driver_manifest.to_excel(driver_manifest_path, index=False)
	for run_number, df in driver_manifest_dict.items():
		driver_manifest_path = os.path.join(DROPBOX_FOLDER, f"Weeks {year}", f"Week {week:02d}", state, "04 - Driver Manifest", f"driver manifest - {run_number}.xlsx")
		df.to_excel(driver_manifest_path, index=False)
	
	detrack_path = os.path.join(DROPBOX_FOLDER, f"Weeks {year}", f"Week {week:02d}",state, "05 - Detrack", f"detrack - {full_state} - {day}.xlsx")
	detrack.to_excel(detrack_path, index=False)
	
	hds_detrack_path = os.path.join(DROPBOX_FOLDER, f"Weeks {year}", f"Week {week:02d}",state, "05 - Detrack", f"detrack HDS - {full_state} - {day}.xlsx")
	hds_detrack.to_excel(hds_detrack_path, index=False)
	
	routed_path = os.path.join(DROPBOX_FOLDER, f"Weeks {year}", f"Week {week:02d}", state, "06 - Routed", f"routed - {full_state} - {day}.xlsx")
	routed.to_excel(routed_path, index=False)

	for customer, customer_df in customer_etas.items():
		if customer == "HelloFresh":
			if is_am:
				day = f"{weekday[:3].upper()} AM"
			else:
				day = weekday[:3].upper()
			hellofresh_folder = os.path.join(DROPBOX_FOLDER, f"Weeks {year}", f"Week {week:02d}", "02 To Customer", "HelloFresh")
			if not os.path.exists(hellofresh_folder):
				os.makedirs(hellofresh_folder)

			if state == "NSW":
				customer_eta_path = os.path.join(hellofresh_folder, f"Balto {day}.xlsx")
			else:
				customer_eta_path = os.path.join(hellofresh_folder, f"Balto {full_state} {day}.xlsx")
		else:
			customer_eta_path = os.path.join(DROPBOX_FOLDER, f"Weeks {year}", f"Week {week:02d}", "02 To Customer", f"To {customer} - {full_state} - {day}.xlsx")
		customer_df.to_excel(customer_eta_path, index=False)


def main(filename, week, year, weekday, state, is_am, warehouse_name, warehouse_address, reg, hds, sync_time="", start_time=""):
	time_slot = TIME_SLOTS_AM[is_am]

	filepath = os.path.join(DROPBOX_FOLDER, f"Weeks {year}", f"Week {week:02d}", state, "03 - From WW", filename)

	if not (filepath.endswith(".xlsx") or filepath.endswith(".xls") or filepath.endswith(".csv")):
		print("Valid extensions for input file are '.csv', '.xls' or '.xlsx'.\n Please enter a valid file.")
		raise
	workbook = xlrd.open_workbook(filepath)
	sheet_name = "Mixed"
	if "Mixed" not in workbook.sheet_names():
		sheet_name = "Sheet0"
	df = read_data(filepath, sheet_name)
	route_area = export_run_list(filename, week, year, state, weekday, is_am, reg, hds, warehouse_name, start_time)
	driver_manifest, driver_manifest_dict = make_driver_manifest(df, year, week, weekday, state, is_am, reg, hds, route_area)
	detrack = make_detrack_file(df, year, week, weekday, state, is_am, warehouse_address, reg, hds, sync_time, route_area)
	hds_detrack = make_hds_detrack_file(df, year, week, weekday, state, is_am, warehouse_address, reg, hds, sync_time, route_area)
	# hds_detrack = make_detrack_file(df, year, week, weekday, state, is_am, warehouse_address, reg, hds)
	routed = make_routed_file(df, year, week, weekday, state, is_am, hds, route_area)
	customer_etas = make_customer_etas(df, year, week, weekday, state, is_am, reg, hds, route_area)
	export_data(driver_manifest, driver_manifest_dict, detrack, hds_detrack, routed, customer_etas, state, weekday, week, year, is_am, reg, hds)


if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument("-f", "--filename", type=str, required=True,
						help="Input file extension should be '.csv', '.xls' or '.xlsx'.")
	parser.add_argument("-y", "--year", type=int, required=True)
	parser.add_argument("-w", "--week", type=int, required=True)
	parser.add_argument("-d", "--weekday", type=str, required=True,
						help="Day of the week. Example: Sunday")
	parser.add_argument("-s", "--state", type=str, required=True,
						help="State. Example: VIC or NSW")
	parser.add_argument("-ad", "--address", type=str, required=True,
						help="Warehouse address")
	parser.add_argument("-a", "--am", type=bool, required=False, default=False,
						help="True if is AM, else False. Default=False.")
	parser.add_argument("-r", "--reg", type=bool, required=False, default=False,
						help="True if is REG, else False. Default=False.")

	args = parser.parse_args()

	filename = args.filename
	week = args.week
	year = args.year
	weekday = args.weekday
	state = args.state
	is_am = args.am
	reg = args.reg
	warehouse_address = args.address

	main(filename, week, year, weekday, state, is_am, warehouse_address, reg, hds=False)
