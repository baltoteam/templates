import argparse
import os
import sys
import pandas as pd
from os.path import dirname, abspath

current_path = os.path.join(dirname(dirname(abspath(__file__))), "python templates")

from collections import defaultdict
from datetime import datetime, time
from tempfile import TemporaryDirectory
from utils.tools import excel_to_csv
from utils.parameters import DROPBOX_FOLDER, ROUTING_DATE, THR1VE_STATES


def delete_empty_lines(filepath):
	rows = []
	with open(filepath, 'r') as f:
		for line in f:
			line_list = line.split(',')[:-1]
			if not set(line_list) == {''}:
				rows.append(line)
	with open(filepath, 'w') as f:
		for line in rows:
			f.write(line)


def add_columns(df, load, service_time, client):
	df['Load'] = [load] * len(df)
	df['Service Time'] = [service_time] * len(df)
	df['Client'] = [client] * len(df)

	for col_name in df.columns:
		if "phone" in col_name.lower():
			df = df.rename(columns = {col_name: "Phone"})
	return df


def merge_customer_name(df):
	first_names = []
	last_names = []
	names = []
	for col_name in df.columns:
		if 'firstname' in col_name.replace(' ', '').replace('_', '').lower():
			first_names = list(df[col_name])
			del df[col_name]
		elif 'lastname' in col_name.replace(' ', '').replace('_', '').lower():
			last_names = list(df[col_name])
			del df[col_name]
	if len(first_names + last_names):
		names = []
		for i in range(len(first_names)):
			first_name = first_names[i]
			last_name = last_names[i]
			if pd.isnull(first_name):
				first_name = ""
			if pd.isnull(last_name):
				last_name = ""
			names.append(f"{first_name} {last_name}")
		df['Customer'] = names


def fix_phone_number(df):
	if "Phone" in df.columns:
		for ind in df.index:
			if pd.isnull(df["Phone"][ind]):
				continue
			number = str(df["Phone"][ind])
			if not number.startswith("0"):
				df.at[ind, "Phone"] = f'0{number}'


def format_file(client, load, service_time, df, outpath, date, week, year, geolocation_state, am):
	df = add_columns(df, load, service_time, client)
	fix_phone_number(df)
	merge_customer_name(df)

	ind_num = df.index

	# Create new formatted dataframe
	header = ["Customer", "Address", "Latitude", "Longitude", "Phone", "Email",
			  "Instructions", "Load", "Service Time", "Client", "Date",
			  "Time Window Start", "Time Window End"]
	formatted_df = pd.DataFrame(columns=header)
	col_names = list(df.columns)
	checked_columns = []
	# Customer
	customer_cols = []
	for col_name in col_names:
		if "customer" in col_name.lower() and not col_name in checked_columns and not "customerid" in col_name.lower().replace(" ", "").replace("_", ""):
			customer_cols.append(col_name)
	if len(customer_cols) == 0:
		customer_cols = col_names
	if len(customer_cols) > 1:
		for col_name in customer_cols:
			if "name" in col_name.lower() and not col_name in checked_columns:
				customer_col = col_name
	elif len(customer_cols) == 1:
		customer_col = customer_cols[0]
	formatted_df["Customer"] = df[customer_col]
	checked_columns.append(customer_col)
	
	# Email
	email_col = None
	for col_name in col_names:
		if "email" in col_name.lower() and not col_name in checked_columns:
			email_col = df[col_name]
			checked_columns.append(col_name)
	
	if email_col is None:
		email_col = [""] * len(ind_num)	
	formatted_df["Email"] = email_col

	# Address
	street, suburb, city, postcode, state = "", "", "", "", geolocation_state
	address_pieces = []

	for col_name in col_names:
		# Street
		if not col_name in checked_columns:
			if "street" in col_name.lower() or "address" in col_name.lower():
				street = col_name
				checked_columns.append(col_name)
				address_pieces.append(street)
			elif "suburb" in col_name.lower():
				suburb = col_name
				checked_columns.append(col_name)
				address_pieces.append(suburb)
			elif "city" in col_name.lower() and not "suburb" in col_name.lower():
				city = col_name
				checked_columns.append(col_name)
				address_pieces.append(city)
			elif "postcode" in col_name.lower().replace("_", "").replace(" ",""):
				postcode = col_name
				checked_columns.append(col_name)
				address_pieces.append(postcode)
			elif "state" in col_name.lower():
				state = col_name
				checked_columns.append(col_name)
				address_pieces.append(state)

	address_col = []

	for ind in ind_num:
		full_address = []
		for piece in address_pieces:
			value = df[piece][ind]
			if pd.isnull(value):
				full_address.append("")
			elif not type(value) == str:
				full_address.append(str(int(value)))
			else:
				full_address.append(value)
		address = ", ".join(full_address)
		address_col.append(address)
	formatted_df["Address"] = address_col

	# Instructions
	for col_name in col_names:
		if not col_name in checked_columns:
			if "instructions" in col_name.lower() or "notes" in col_name.lower() or "comment" in col_name.lower():
				instruction_column = col_name
				instructions = []
				for ind in df.index:
					if "N/A" in str(df[instruction_column][ind]):
						instructions.append("")
					else:
						instructions.append(df[instruction_column][ind])
				formatted_df["Instructions"] = instructions
				checked_columns.append(instruction_column)
			if "company" in col_name.lower():
				company_column = col_name
				checked_columns.append(company_column)
	try:
		formatted_df["Instructions"] = [". ".join([df[company_column].fillna("")[ind], df[instruction_column].fillna("")[ind]]) for ind in df.index]
		formatted_df["Instructions"] = formatted_df["Instructions"].replace(". ", "")
	except:
		pass

	# Phone number
	for col_name in col_names:
		if "phone" in col_name.lower():
			formatted_df["Phone"] = df[col_name]

	# Phone, Load, Service Time & Client
	formatted_df["Load"] = df["Load"]

	formatted_df["Service Time"] = df["Service Time"]
	formatted_df["Client"] = df["Client"]
	formatted_df["Date"] = [date for i in range(len(df))]
	formatted_df["Latitude"], formatted_df["Longitude"] = geolocation(df, formatted_df["Customer"], formatted_df["Address"], week, year, geolocation_state)


	# Add time windows
	if client == "Thr1ve":
		# formatted_df["Time Window Start"] = [slot.split("-")[0][:2] + ":" + slot.split("-")[0][2:] for slot in df["Time Slot"]]
		# formatted_df["Time Window End"] = [slot.split("-")[1][:2] + ":" + slot.split("-")[1][2:] for slot in df["Time Slot"]]
		formatted_df["Latitude"], formatted_df["Longitude"] = df["Latitude"], df["Longitude"]	
	if am:
		formatted_df["Time Window Start"] = [time(0, 0) for ind in range(len(formatted_df))]
		formatted_df["Time Window End"] = [time(7, 0) for ind in range(len(formatted_df))]
	else:
		formatted_df["Time Window Start"] = [time(7, 0) for ind in range(len(formatted_df))]
		formatted_df["Time Window End"] = [time(17, 0) for ind in range(len(formatted_df))]

	# Delete duplicates
	formatted_df = formatted_df.drop_duplicates(subset=["Customer", "Address"])
	print(f"{os.path.basename(outpath)}: {len(formatted_df)}")
	formatted_df.to_excel(outpath, index=False, na_rep="")


def geolocation(input_df, df_name, df_address, week, year, state):
	# Build database over the last 3 weeks
	database_list = []
	if week == 1:
		weeks = [(year - 1, 50), (year - 1, 51), (year - 1, 52)]
	elif week == 2:
		weeks = [(year - 1, 51), (year - 1, 52), (year, week - 1)]
	elif week == 3:
		weeks = [(year - 1, 52), (year, week - 2), (year, week - 1)]
	else:
		weeks = [(year, week - 3), (year, week - 2), (year, week - 1)]
	for yr, wk in weeks:
		for f in os.listdir(os.path.join(DROPBOX_FOLDER, f"Weeks {yr}", f"Week {wk:02d}", state, "03 - From WW")):
			if f.startswith("From WW"):
				try:
					df = pd.read_excel(os.path.join(DROPBOX_FOLDER, f"Weeks {yr}", f"Week {wk:02d}", state, "03 - From WW", f), sheet_name="Sheet0")
					df = df[df["Type"] == "delivery"]
					db = pd.DataFrame()
					db["id"] = df["Name"] + df["Address"]
					db["Latitude"] = df["Latitude"]
					db["Longitude"] = df["Longitude"]
					database_list.append(db)
				except:
					pass
	database = pd.concat(database_list).drop_duplicates().set_index("id")
	df_id = df_name + df_address
	latitude = []
	longitude = []
	for id_ in df_id:
		try:
			latitude.append(float(database["Latitude"][id_]))
			longitude.append(float(database["Longitude"][id_]))
		except:
			latitude.append("")
			longitude.append("")
	return latitude, longitude


def updated_time_windows(df, timeslot_dict, issues_dict):
	time_window_start = []
	time_window_end = []
	for ind in df.index:
		customer = df["Customer"][ind]
		time_window = df["Time window"][ind]
		if time_window in timeslot_dict.keys():
			tw_start = datetime.strptime(timeslot_dict[time_window]["start"], "%H:%M").time()
			tw_end = datetime.strptime(timeslot_dict[time_window]["end"], "%H:%M").time()
		else:
			tw_start = datetime.strptime(df["Window Start"][ind], "%H:%M").time()
			tw_end = datetime.strptime(df["Window End"][ind], "%H:%M").time()
		if customer in issues_dict.keys():
			if not pd.isnull(issues_dict[customer]["Time Window Start"]):
				tw_start = issues_dict[customer]["Time Window Start"]
			if not pd.isnull(issues_dict[customer]["Time Window End"]):
				tw_end = issues_dict[customer]["Time Window End"]
		time_window_start.append(tw_start)
		time_window_end.append(tw_end)
	return time_window_start, time_window_end


def delivery_comments(df, issues_dict):
	delivery_comments = []
	for ind in df.index:
		customer = df["Customer"][ind]
		if customer in issues_dict.keys() and not pd.isnull(issues_dict[customer]["Instructions"]):
			delivery_comment = df["Delivery comment"][ind] + ". " + issues_dict[customer]["Instructions"]
		else:
			delivery_comment = df["Delivery comment"][ind]
		delivery_comments.append(delivery_comment)
	return delivery_comments


def format_HelloFresh(load, service_time, date, state, df, outpath, timeslot_dict, week, year):
	# Fix recurring delivery issues
	issues_dict = load_recurring_delivery_issues()

	formatted_df = pd.DataFrame(columns=["ID", "Customer", "Street",
										 "Time Window Start", "Time Window End",
										 "Postcode", "City", "State", "Country",
										 "Phone", "Delivery comment", "Type",
										 "load", "Service Time", "Client",
										 "Latitude", "Longitude", "Address",
										 "D.O", "HF TWS", "HF TWE", "Date"])

	formatted_df["ID"] = df["ID"]
	formatted_df["Customer"] = df["Customer"]
	formatted_df["Street"] = df["Street"]
	formatted_df["Time Window Start"] = updated_time_windows(df, timeslot_dict, issues_dict)[0]
	formatted_df["Time Window End"] = updated_time_windows(df, timeslot_dict, issues_dict)[1]
	formatted_df["Postcode"] = df["Postcode"]
	formatted_df["City"] = df["City"]
	formatted_df["State"] = [state for i in range(len(df))]
	formatted_df["Country"] = ["Australia" for i in range(len(df))]
	formatted_df["Phone"] = df["Phone"]
	formatted_df["Delivery comment"] = delivery_comments(df, issues_dict)
	formatted_df["Type"] = df["Box Types"]
	formatted_df["load"] = [load for i in range(len(df))]
	formatted_df["Service Time"] = [service_time for i in range(len(df))]
	formatted_df["Client"] = ["HelloFresh" for i in range(len(df))]
	formatted_df["Address"] = [f"{df['Street'][ind]}, {df['City'][ind]}, {state}, {df['Postcode'][ind]}" for ind in df.index]
	formatted_df["D.O"] = df["D.O."]
	formatted_df["HF TWS"] = df["Window Start"]
	formatted_df["HF TWE"] = df["Window End"]
	formatted_df["Date"] = [date for i in range(len(df))]

	formatted_df["Latitude"], formatted_df["Longitude"] = geolocation(df, formatted_df["Customer"], formatted_df["Address"], week, year, state)
	# Delete duplicates
	formatted_df = formatted_df.drop_duplicates(subset=["Customer", "Address"])
	print(f"{os.path.basename(outpath)}: {len(formatted_df)}")
	formatted_df.to_excel(outpath, index=False, na_rep="")


def format_Thr1ve(load, service_time, filepath, year, week, am):
	df = pd.read_excel(filepath, sheet_name="Paste Export Here", skip_blank_lines=True)
	df = df.dropna(axis=0, how='all').dropna(axis=1, how='all')

	df["State"] = [THR1VE_STATES[zone] for zone in df["Zone Name"]]
	is_am = []
	service_time_list = []
	for time_slot in df["Time Slot"]:
		if int(str(time_slot)[:2]) < 5:
			is_am.append(" AM")
			service_time_list.append(3)
		else:
			is_am.append("")
			service_time_list.append(4)
	df["AM"] = is_am
	df["Service Time"] = service_time_list
	is_reg = []
	for zone in df["Zone Name"]:
		if zone in {"NEWCASTLE", "NSW WOLLONGONG", "NSW CENTRAL"}:
			is_reg.append(" REG")
		else:
			is_reg.append("")
	df["REG"] = is_reg
	df["Area"] = df["Delivery Day"] + df["AM"] + " - " + df["State"] + df["REG"]

	area_dict = df.groupby("Area")
	for area in area_dict.groups:
		area_df = area_dict.get_group(area)
		formatted_df = pd.DataFrame(columns=["ID", "Contact Name", "Instructions",
											 "Phone", "Address", "Latitude", "Longitude",
											 "Suburb", "Post Code", "Company", "Email",
											 "Time Window End", "Delivery Day", "Service Time",
											 "Load", "State", "Client", "Date", "Time Window Start"])
		formatted_df["ID"] = area_df["Serial Number"]
		formatted_df["Contact Name"] = area_df["Contact Name"]
		formatted_df["Instructions"] = area_df["Special Instructions"]
		formatted_df["Phone"] = area_df["Phone"]
		formatted_df["Address"] = area_df["Address"]
		formatted_df["Suburb"] = area_df["Suburb"]
		formatted_df["Post Code"] = area_df["Post Code"]
		formatted_df["Company"] = area_df["Company"]
		formatted_df["Email"] = area_df["Email"]
		formatted_df["Time Window End"] = [slot.split("-")[1][:2] + ":" + slot.split("-")[1][2:] for slot in area_df["Time Slot"]]
		formatted_df["Delivery Day"] = area_df["Delivery Day"]
		formatted_df["Service Time"] = area_df["Service Time"]
		formatted_df["Load"] = [load for i in range(len(area_df))]
		formatted_df["State"] = area_df["State"]
		formatted_df["Client"] = ["THR1VE" for i in range(len(area_df))]
		state = area_df.reset_index()["State"][0]
		reg = area_df.reset_index()["REG"][0]
		day = area_df.reset_index()["Delivery Day"][0]
		am = area_df.reset_index()["AM"][0]

		date = ROUTING_DATE[state + reg][day + am]
		formatted_df["Date"] = [date for i in range(len(area_df))]
		addresses = pd.Series([f"{formatted_df['Address'][ind]}, {formatted_df['Suburb'][ind]}, {formatted_df['State'][ind]}, {int(formatted_df['Post Code'][ind])}" for ind in formatted_df.index])
		formatted_df["Latitude"], formatted_df["Longitude"] = geolocation(area_df, formatted_df.reset_index()["Contact Name"], addresses, week, year, state)
		outpath = os.path.join(DROPBOX_FOLDER, f"Weeks {year}", f"Week {week:02d}", state, "02 - To WW", f"To WW - {area} - Thr1ve.xlsx")
		# Delete duplicates
		formatted_df = formatted_df.drop_duplicates(subset=["Contact Name", "Address", "Suburb", "Post Code"])
		print(f"{os.path.basename(outpath)}: {len(formatted_df)}")
		# Add time windows
		if am:
			formatted_df["Time Window Start"] = [time(0, 0) for ind in range(len(formatted_df))]
		else:
			formatted_df["Time Window Start"] = [time(7, 0) for ind in range(len(formatted_df))]
		formatted_df.to_excel(outpath, index=False, na_rep="")


def load_recurring_delivery_issues():
	issues_df = pd.read_excel(os.path.join("databases", "Recurring Delivery issues.xlsx"))
	issues_dict = defaultdict(lambda: {
			"Time Window Start": None,
			"Time Window End": None,
			"Instructions": None,
		})
	for ind in issues_df.index:
		client = issues_df["Customer"][ind]
		issues_dict[client]["Time Window Start"] = issues_df["Time Window Start"][ind]
		issues_dict[client]["Time Window End"] = issues_df["Time Window End"][ind]
		issues_dict[client]["Instructions"] = issues_df["Instructions"][ind]
	return issues_dict


def main(client, load, service_time, filename, week, year, state, is_am, is_reg, weekday, timeslot_dict):
	if is_am:
		weekday = weekday + " AM"

	# To add to To WW file name if is regional
	if is_reg:
		reg = " REG"
	else:
		reg = ""

	filepath = os.path.join(DROPBOX_FOLDER, f"Weeks {year}", f"Week {week:02d}", "01 From Customer", client, filename)
	outpath = os.path.join(DROPBOX_FOLDER, f"Weeks {year}", f"Week {week:02d}", state, "02 - To WW", f"To WW - {weekday} - {state}{reg} - {client}.xlsx")

	if filepath.endswith(".xls") or filepath.endswith(".xlsx"):
		input_df = pd.read_excel(filepath, skip_blank_lines=True)
	elif filepath.endswith(".csv"):
		try:
			input_df = pd.read_csv(filepath, skip_blank_lines=True)
		except UnicodeDecodeError:
			input_df = pd.read_csv(filepath, skip_blank_lines=True, encoding="ISO-8859-1")
	else:
		print("Valid extensions for input file are '.csv', '.xls' or '.xlsx'.\n Please enter a valid file.")
		raise
	# Remove blank rows and blank columns
	input_df = input_df.dropna(axis=0, how='all')

	if client == "HelloFresh":
		format_HelloFresh(load, service_time, ROUTING_DATE[state + reg][weekday], state, input_df, outpath, timeslot_dict, week, year)
	elif client == "Thr1ve":
		format_Thr1ve(load, service_time, filepath, year, week, is_am)
	else:
		format_file(client, load, service_time, input_df, outpath, ROUTING_DATE[state + reg][weekday], week, year, state, is_am)


if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument("-f", "--filename", type=str, required=True,
						help="Input file extension should be '.csv', '.xls' or '.xlsx'.")
	parser.add_argument("-c", "--client", type=str, required=True)
	parser.add_argument("-s", "--servicetime", type=int, required=False,
						default=4)
	parser.add_argument("-w", "--week", type=int, required=True)
	parser.add_argument("-y", "--year", type=int, required=True)
	parser.add_argument("-a", "--am", type=bool, required=False, default=False,
						help="True if AM, False otherwise. Default is False.")
	parser.add_argument("-r", "--reg", type=bool, required=False, default=False,
						help="True if Regional, False otherwise. Default is False.")
	parser.add_argument("-z", "--zone", type=str, required=True, help="Example: VIC or NSW")
	parser.add_argument("-l", "--load", type=int, required=False,
						default=1)
	parser.add_argument("-d", "--weekday", type=str, required=True,
						help="Day of the week. Example: Sunday")

	args = parser.parse_args()

	client = args.client
	load = args.load
	service_time = args.servicetime
	filename = args.filename
	week = args.week
	year = args.year
	state = args.zone
	is_am = args.am
	is_reg = args.reg
	weekday = args.weekday

	main(client, load, service_time, filename, week, year, state, is_am, is_reg, weekday, {})