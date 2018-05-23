import argparse
import pandas as pd
import numpy as np
import os
import sys

from collections import defaultdict
from datetime import datetime
from openpyxl import load_workbook
from tempfile import TemporaryDirectory
from os.path import dirname, abspath
from utils.parameters import DROPBOX_FOLDER  


sys.path.append(os.path.join(dirname(dirname(abspath(__file__))), "python templates"))
from utils.tools import excel_to_csv, df_to_excel

def parse_input_drop_numbers(input_df, state, day, is_am):
	drop_nums = defaultdict(lambda: {
			"vehicle": None,
			"drop #": 0,
			"eta": "",
		})
	input_clients = set()
	vehicles = set()

	if is_am:
		am = " AM"
	else:
		am = ""

	for ind in input_df.index:
		if input_df["Type"][ind] == "delivery":
			name = input_df["Name"][ind]
			client = input_df["client"][ind]
			address = input_df["Address"][ind]
			drop_id = f"{name}_{client}_{address}"  # example: Emma Leiner_HelloFresh
			input_clients.add(drop_id)
			drop_nums[drop_id]["vehicle"] = input_df["Vehicle"][ind].replace("Balto", f"{day[:3]} {state}{am}")
			drop_nums[drop_id]["drop #"] = float(input_df["Step Number"][ind])
			# drop_nums[drop_id]["eta"] = datetime.strptime(input_df["Current Schedule"][ind], "%I:%M %p").time()
			drop_nums[drop_id]["eta"] = input_df["Current Schedule"][ind]
			vehicles.add(int(input_df["Vehicle"][ind].split(" ")[-1]))
	return drop_nums, input_clients, vehicles


def get_drop_id(df, ind):
	name = df["Name"][ind]
	client = df["client"][ind]
	address = df["Address"][ind]
	return f"{name}_{client}_{address}"


def fix_vehicle_name(output_df, drop_nums, input_clients, ind, previous_vehicle, state, day, is_am):
	drop_id = get_drop_id(output_df, ind)
	if is_am:
		am = " AM"
	else:
		am = ""
	if drop_id in input_clients:
		vehicle = drop_nums[drop_id]["vehicle"]
	else:
		if output_df["Type"][ind] in {"departure", "arrival"} or previous_vehicle is None:
			vehicle = output_df["Vehicle"][ind].replace("Balto", f"{day[:3]} {state}{am}")
		elif output_df["Type"][ind] == "delivery":
			vehicle = previous_vehicle

	output_df.at[ind, "Vehicle"] = vehicle
	return vehicle


def replace_output_drop_numbers(output_df, drop_nums, input_clients, state, day, is_am, vehicles):
	vehicle = None
	drop_number = 0.
	eta = datetime(2000, 1, 1, 0, 0).time().strftime("%I:%M %p")
	for ind in output_df.index:
		vehicle_num = int(output_df["Vehicle"][ind].split(" ")[-1])
		if output_df["Type"][ind] in {"departure", "arrival"}:
			output_df.at[ind, "Step Number"] = 0.
		elif output_df["Type"][ind] == "delivery" and vehicle_num not in vehicles:  # new run
			output_df.at[ind, "Step Number"] = drop_number + 1
		else:
			drop_id = get_drop_id(output_df, ind)
			if drop_id not in input_clients:
				output_df.at[ind, "Current Schedule"] = eta
				if drop_number == 0.:
					output_df.at[ind, "Step Number"] = round(drop_number + 0.1, 2)
				else:
					if drop_number != 0. and (drop_number % int(drop_number)) == 0.:
						output_df.at[ind, "Step Number"] = round(drop_number + 0.1, 2)
					else:
						output_df.at[ind, "Step Number"] = round(drop_number + 0.01, 2)
			else:
				output_df.at[ind, "Step Number"] = drop_nums[drop_id]["drop #"]
				output_df.at[ind, "Current Schedule"] = drop_nums[drop_id]["eta"]
		eta = output_df["Current Schedule"][ind]
		vehicle = fix_vehicle_name(output_df, drop_nums, input_clients, ind, vehicle, state, day, is_am)
		drop_number = output_df.at[ind, "Step Number"]


def mixing(input_file, output_file, state, day, is_am):
	drop_number = 0
	input_df = pd.read_excel(input_file)
	output_df = pd.read_excel(output_file)

	drop_nums, input_clients, vehicles = parse_input_drop_numbers(input_df, state, day, is_am)

	replace_output_drop_numbers(output_df, drop_nums, input_clients, state, day, is_am, vehicles)

	# output_df.to_csv(output_file, index=False)
	df_to_excel(output_df, output_file, "Mixed")


def main(week, year, state, day, is_am, base_input, base_output):
	#ops_folder = f"C:/Users/custo/Dropbox (Balto Team)/Balto Team team folder/Operations & Tech/Weeks {year}/Week {week:02d}" 
	ops_folder = f"{DROPBOX_FOLDER}/Weeks {year}/Week {week:02d}"

	with TemporaryDirectory() as tmp:
		input_file_path = f"{ops_folder}/{state}/03 - From WW/{base_input}"
		output_file_path = f"{ops_folder}/{state}/03 - From WW/{base_output}"

		mixing(input_file_path, output_file_path, state, day, is_am)


if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument("-i", "--inputname", type=str, required=True,
						help="Input file with reference drop numbers. Insert a name, not a path. Example: 'From WW - Monday - QLD - HF.xlsx'")
	parser.add_argument("-o", "--outputname", type=str, required=True,
						help="Output file where to update drop numbers. Insert a name, not a path. Example: 'From WW - Monday - QLD - HF+TH1.xlsx'")
	parser.add_argument("-w", "--week", type=int, required=True,
						help="Week number. Example: 8")
	parser.add_argument("-y", "--year", type=int, required=True,
						help="Year. Example: 2018")
	parser.add_argument("-s", "--state", type=str, required=True,
						help="Routed state. Example: 'NSW'")
	parser.add_argument("-d", "--day", type=str, required=True,
						help="Routed weekday. Example: 'Tuesday' or 'Tue'")
	parser.add_argument("-am", "--am", type=bool, required=False, default=False,
						help="Defines whether deliveries are in AM or not. Default is False.")
	args = parser.parse_args()

	week = args.week
	year = args.year
	state = args.state
	day = args.day
	is_am = args.am
	base_input = args.inputname
	base_output = args.outputname

	main(week, year, state, day, is_am, base_input, base_output)