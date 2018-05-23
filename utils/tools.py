# -*- coding: utf-8 -*-

import csv
import os
import xlrd
import pandas as pd

from os.path import join
from openpyxl import load_workbook
from datetime import datetime, timedelta

from utils.parameters import DROPBOX_FOLDER

NUMBER_TO_DAY = {
	0: "Monday",
	1: "Tuesday",
	2: "Wednesday",
	3: "Thursday",
	4: "Friday",
	5: "Saturday",
	6: "Sunday"
}


DAY_TO_NUMBER = {
	"Monday": 0,
	"Tuesday": 1,
	"Wednesday": 2,
	"Thursday": 3,
	"Friday": 4,
	"Saturday": 5,
	"Sunday": 6,
}


def build_day_calendar(year):
	days_of_week = set()
	first_week = {}
	current_day = datetime(year=year, month=1, day=1)
	while current_day.weekday() != 6:  # get first Sunday of the year
		current_day -= timedelta(days=1)
	while len(days_of_week) < 7:
		first_week[current_day.weekday()] = current_day
		days_of_week.add(current_day.weekday())
		current_day += timedelta(days=1)		
	return first_week


def find_date(year, week, weekday):
	calendar = build_day_calendar(year)
	return (calendar[DAY_TO_NUMBER[weekday]] + timedelta(days=(week - 1) * 7)).date()


def find_week_dates(year, week):
	weekdays = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]
	weekdates = []
	for day in weekdays:
		weekdates.append(str(find_date(year, week, day)))
	return weekdates


def xrange(x):
    return iter(range(x))


def excel_to_csv(excel_file, csv_path=None, sheet_names=None):
	csv_files = []

	workbook = xlrd.open_workbook(excel_file)
	basepath = ('.').join(excel_file.split('.')[:-1])
	if sheet_names is None:
		sheet_names = workbook.sheet_names()
	multi_sheets = len(sheet_names) > 1

	for sheet_name in sheet_names:
		# If the sheet name enterred is wrong, just use the regular "Sheet0" name
		if len(sheet_names) == 1 and sheet_name not in workbook.sheet_names():
			sheet_name = "Sheet0"
		sh = workbook.sheet_by_name(sheet_name)
		if multi_sheets:
			sheet_name = sheet_name.replace(" ", "_")
			if csv_path is None:
				csv_path = f"{basepath}_{sheet_name}.csv"
			else:
				base_csv_path = ('.').join(csv_path.split('.')[:-1])
				csv_path = f"{base_csv_path}_{sheet_name}.csv"
		else:
			if csv_path is None:
				csv_path = f"{basepath}.csv"
		csv_files.append(csv_path)

		csv_file = open(csv_path, "w", newline="\n", encoding='utf-8')
		wr = csv.writer(csv_file, quoting=csv.QUOTE_ALL)

		for rownum in xrange(sh.nrows):
			wr.writerow([c for c in sh.row_values(rownum)])

		csv_file.close()

	return csv_files


def df_to_excel(df, output_file, sheet_name):
	output_book = load_workbook(output_file)
	writer = pd.ExcelWriter(output_file, engine='openpyxl')
	writer.book = output_book
	writer.sheets = dict((ws.title, ws) for ws in output_book.worksheets)
	df.to_excel(writer, sheet_name, index=False)
	writer.save()


def merge_customer_files(year, week, client, file_name_list, new_file_name):
	file_path_list = [os.path.join(DROPBOX_FOLDER, f"Weeks {year}", f"Week {week:02d}",
								   "01 From Customer", client, file_name) for file_name in file_name_list if file_name != ""]
	new_file_path = os.path.join(DROPBOX_FOLDER, f"Weeks {year}", f"Week {week:02d}",
								 "01 From Customer", client, new_file_name)
	new_df = pd.concat([pd.read_excel(file_path) for file_path in file_path_list])
	new_df.to_excel(new_file_path, index=False)


def merge_from_ww_files(year, week, state, file_name_list, new_file_name):
	file_path_list = [os.path.join(DROPBOX_FOLDER, f"Weeks {year}", f"Week {week:02d}",
								   state, "03 - From WW", file_name) for file_name in file_name_list if file_name != ""]
	new_file_path = os.path.join(DROPBOX_FOLDER, f"Weeks {year}", f"Week {week:02d}",
								 state, "03 - From WW", new_file_name)
	new_df = pd.concat([pd.read_excel(file_path) for file_path in file_path_list]).reset_index()
	step_numbers = []
	for ind in new_df.index:
		if new_df["Type"][ind] == "delivery":
			step_numbers.append(new_df["Step Number"][ind])
		else:
			step_numbers.append(0)
	new_df["Step Number"] = step_numbers
	new_df = new_df.sort_values(by=["Vehicle", "Step Number"])
	new_df.to_excel(new_file_path, index=False)