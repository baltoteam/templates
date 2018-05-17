import os
import pandas as pd
import sys
import xlrd

from collections import defaultdict
from datetime import datetime
from os.path import dirname, abspath

sys.path.append(os.path.join(dirname(dirname(abspath(__file__))), "python templates"))
from utils.tools import excel_to_csv
from utils.parameters import DROPBOX_FOLDER
from templates.driver_assigning import assign_drivers_and_contractors


input_header = ["Vehicle", "Driver", "Date", "Step Number",
				"Stop Number", "Order Number", "Current Schedule",
				"Planned Vehicle", "Planned Schedule", "Type",
				"Time In", "Time Out", "Auto Time In",
				"Auto Time Out", "Status", "Status Time",
				"Name", "Address", "Latitude", "Longitude",
				"Distance to Next Km", "Drive Time to Next",
				"Idle Time Here", "Distance Km Left",
				"Tot Working Time", "Driving Time Left",
				"Idle Time Left", "Tot Service Time",
				"Tot Stops", "Tot Orders", "Notes",
				"Vehicle Tags", "Required Tags", "Banned Tags",
				"Service Time", "Driver Note", "Load load",
				"client", "company", "d.o", "email",
				"hf twe", "hf tws", "id", "phone",
				"Time Window Start", "Time Window End"]


output_header = ["vehicle", "driver", "date", "step number",
				 "stop number", "order number", "current schedule",
				 "planned vehicle", "planned schedule", "type",
				 "time in", "time out", "auto time in",
				 "auto time out", "status", "status time",
				 "name", "address", "latitude", "longitude",
				 "distance to next km", "drive time to next",
				 "idle time here", "distance km left",
				 "tot working time", "driving time left",
				 "idle time left", "tot service time",
				 "tot stops", "tot orders", "notes",
				 "vehicle tags", "required tags", "banned tags",
				 "service time", "driver note", "load load",
				 "client", "company", "email", "id",
				 "instructions1", "phone", "plan_info",
				 "time window start", "time window end"]


def default_to_regular(d):
    if isinstance(d, defaultdict):
        d = {k: default_to_regular(v) for k, v in d.items()}
    return d


def get_area(address, hds):
	splitted_address = address.split(',')
	try:
		if hds:
			area = splitted_address[-4].rstrip(" ").lstrip(" ").lower()
		else:
			area = splitted_address[-3].rstrip(" ").lstrip(" ").lower()
	except:
		area = ""
	try:
		number = int(area.split(' ')[0])
		# check if the area starts with a number: if yes, it is a mistake
		return splitted_address[-2].rstrip(" ").lstrip(" ").lower() # get rid of useless spaces
	except:
		# if the area does not start with a number, it is more likely we got the right area
		return area.rstrip(" ").lstrip(" ").lower()


def get_route_names(df, weekday, state, is_am, reg, hds):
	if hds:
		return df["Vehicle"]
	route_names = []
	prefix = f"{weekday[:3]} {state}"
	if reg:
		prefix = prefix + " REG"
	if is_am:
		prefix = prefix + " AM"
	for ind in df.index:
		run_number = str(df["Vehicle"][ind]).split(" ")[-1]
		route_names.append(f"{prefix} {run_number}")
	return route_names


def get_route_names2(df, weekday, state, is_am, reg, hds):
	route_names = []
	prefix = f"{weekday[:3]} {state}"
	if reg:
		prefix = prefix + " REG"
	if is_am:
		prefix = prefix + " AM"
	if hds:
		run_number = 0
		prev_route_name = ""
		for route_name in df["Vehicle"]:
			if route_name != prev_route_name:
				run_number += 1
			route_names.append(f"{prefix} {run_number:02d}")
			prev_route_name = route_name
	else:
		for ind in df.index:
			run_number = str(df["Vehicle"][ind]).split(" ")[-1]
			route_names.append(f"{prefix} {run_number}")
	return route_names



def make_run_dictionary(df, city, day, week, year, state, is_am, reg, hds, warehouse, start_time):
	df = assign_drivers_and_contractors(df, week, year)
	run_dict = defaultdict(lambda: defaultdict(lambda: defaultdict()))
	area_dict = defaultdict(lambda: defaultdict(int))
	# if not hds:
	df["Route code"] = get_route_names(df, day, state, is_am, reg, hds)
	# run_dict template: {
	# 	Run Name: {
	# 		Company: {
	# 			Value Type: Value
	# 		}
	# 	}
	# }
	# Example: {
	# 	"Sun VIC 01": {
	# 		"HelloFresh": {
	# 			"Location": "Sydney",
	# 			"Kilometers": 97,
	# 			"Drops": 53,
	# 		}
	# 	}
	# }

	
	# Fill run_dict	
	for ind in df.index:
		if df["Type"][ind] != "delivery":
			kilometers = df["Distance Km Left"][ind]
			continue
		route_code = df["Route code"][ind]
		route_area = get_area(df["Address"][ind], hds) # get rid of useless spaces
		company = df["client"][ind]
		driver = df["Driver"][ind]
		contractors = df["Contractor"][ind]

		if route_code not in run_dict.keys():
			run_dict[route_code][company]["drops"] = 0
		else:
			if company not in run_dict[route_code].keys():
				run_dict[route_code][company]["drops"] = 0

		area_dict[route_code][route_area] += 1

		run_dict[route_code][company]["Location"] = city
		run_dict[route_code][company]["Day"] = day
		run_dict[route_code][company]["Type"] = "Standard"
		run_dict[route_code][company]["Route Code"] = route_code
		run_dict[route_code][company]["Company"] = company
		if start_time == "":
			run_dict[route_code][company]["Pick-up time"] = start_time
		else:
			run_dict[route_code][company]["Pick-up time"] = datetime.strptime(start_time, "%H:%M").time()
		run_dict[route_code][company]["drops"] += 1
		run_dict[route_code][company]["Warehouse"] = warehouse
		run_dict[route_code][company]["Driver"] = driver
		run_dict[route_code][company]["Contractor"] = contractors
		run_dict[route_code][company]["Kilometers"] = kilometers
	return run_dict, area_dict


def find_route_area(route_areas):
	top3_route_areas = sorted(route_areas.items(), key=lambda x: x[1], reverse=True)[:3]
	return "/".join([area[0].upper() for area in top3_route_areas])


def assign_route_areas(run_dict):
	route_area = defaultdict(str)
	for route_code, areas in run_dict.items():
		route_area[route_code] = find_route_area(areas)
	return route_area


def export_run_list(input_file, week, year, state, day, is_am, reg, hds, warehouse, start_time):
	input_file_path = os.path.join(DROPBOX_FOLDER, f"Weeks {year}", f"Week {week:02d}", state, "03 - From WW", input_file)
	output_name = f"{day} {state}"
	if is_am:
		output_name += " AM"
	if reg:
		output_name += " REG"
	output_file = os.path.join(DROPBOX_FOLDER, f"Weeks {year}", f"Week {week:02d}", "Run Lists", f"run list - {output_name}.xlsx")
	df = pd.read_excel(input_file_path, sheet_name="Sheet0")
	if hds:
		df = df.rename(columns={"client name": "client"})
		df["Address"] = [address.rstrip(" Australia") for address in df["Address"]]
	
	# get city
	if state == "NSW":
		if reg:
			city = "Newcastle/Central Coast"
		else:
			city = "Sydney"
	elif state == "VIC":
		city = "Melbourne"
	elif state == "QLD":
		city = "Brisbane"
	elif state == "ACT":
		city = "Canberra"
	else:
		city = ""

	run_dict, area_dict = make_run_dictionary(df, city, day, week, year, state, is_am, reg, hds, warehouse, start_time)
	route_area = assign_route_areas(area_dict)
	header = ["Location", "Day", "Type", "Route Code", "Contractor",
			  "Driver", "Area", "Company", "Pick-up time", "drops",
			  "Warehouse", "Kilometers"]
	output_df = pd.DataFrame(columns=header)
	for route_code, company_dict in run_dict.items():
		for company, vals in company_dict.items():
			row_to_write = [
				str(vals["Location"]),
				str(vals["Day"]),
				str(vals["Type"]),
				str(vals["Route Code"]),
				str(vals["Contractor"]),
				str(vals["Driver"]),
				str(route_area[route_code]),
				str(vals["Company"]),
				str(vals["Pick-up time"]),
				vals["drops"],
				str(vals["Warehouse"]),
				vals["Kilometers"],
			]
			output_df = output_df.append(pd.DataFrame([row_to_write], columns=header), ignore_index=True)
	output_df.to_excel(output_file, index=False)
