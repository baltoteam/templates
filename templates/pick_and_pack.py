import os
import sys
import pandas as pd
import xlrd

from collections import defaultdict
from openpyxl import load_workbook
from os.path import dirname, abspath
from utils.tools import df_to_excel
from utils.parameters import DROPBOX_FOLDER, DELIVERY_SCHEDULE, PICK_AND_PACK_ZONES

sys.path.append(os.path.join(dirname(dirname(abspath(__file__))), "python templates"))

def make_customer_dict(file_dict, client="Thr1ve"):
	customer_dict = defaultdict(lambda: defaultdict(str))
	# input must be a dictionary of file names, with all the files containing the customer's deliveries
	for key, f in file_dict.items():
		workbook = xlrd.open_workbook(f)
		sheet_name = "Mixed"
		if "Mixed" not in workbook.sheet_names():
			sheet_name = "Sheet0"
		df = pd.read_excel(f, sheet_name=sheet_name)
		
		# Fix vehicles names
		vehicles = []
		for vehicle in df["Vehicle"]:
			number = vehicle[-2:]
			vehicles.append(f"{key} {number}")
		df["Vehicle"] = vehicles

		# Filter clientwise
		df["client"] = [str(cli).lower() for cli in df["client"]]
		df = df[df["client"] == client.lower()]

		# Add drop number and run number for each id-name
		for ind in df.index:
			try:
				customer_id = df["Name"][ind] + str(int(df["id"][ind]))
			except:
				customer_id = df["Name"][ind] + str(df["id"][ind])
			customer_dict[customer_id]["Route Number"] = df["Vehicle"][ind]
			customer_dict[customer_id]["Step Number"] = df["Step Number"][ind]
	return customer_dict


def fill_pick_and_pack_sheet(pick_and_pack_file, sheet_name, customer_dict, df_dictionary):
	df = pd.read_excel(pick_and_pack_file, sheet_name=sheet_name)
	drop_numbers = []
	route_numbers = []

	for ind in df.index:
		try:
			customer_id = df["NAME"][ind] + str(int(df["ID"][ind]))
		except:
			customer_id = df["NAME"][ind] + str(df["ID"][ind])
		if customer_id in customer_dict.keys():
			drop_numbers.append(customer_dict[customer_id]["Step Number"])
			route_numbers.append(customer_dict[customer_id]["Route Number"])
		else:
			if df["Drop"][ind] != "":
				drop_numbers.append(df["Drop"][ind])
			else:
				drop_numbers.append("")
			if df["Route"][ind] != "":
				route_numbers.append(df["Route"][ind])
			else:
				route_numbers.append("")
	df["Drop"] = drop_numbers
	df["Route"] = route_numbers
	df_dictionary[sheet_name] = df


def fill_pick_and_pack(pick_and_pack_file, file_dict, client="Thr1ve"):
	df_dictionary = {}
	customer_dict = make_customer_dict(file_dict, client)
	sheets = PICK_AND_PACK_ZONES[client]
	for sheet in sheets:
		try:
			fill_pick_and_pack_sheet(pick_and_pack_file, sheet, customer_dict, df_dictionary)
		except:
			continue
	export_pick_and_pack(df_dictionary, pick_and_pack_file)


def export_pick_and_pack(df_dictionary, output_file):
	output_book = load_workbook(output_file)
	writer = pd.ExcelWriter(output_file, engine='openpyxl')
	writer.book = output_book
	writer.sheets = dict((ws.title, ws) for ws in output_book.worksheets)
	for sheet_name, df in df_dictionary.items():
		df.to_excel(writer, sheet_name, index=False)
		writer.save()
