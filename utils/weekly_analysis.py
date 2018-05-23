import argparse
import json
import numpy as np
import os
import pandas as pd
import requests
import sys

from collections import defaultdict
from os.path import dirname, abspath
from utils.parameters import DROPBOX_FOLDER
from utils.tools import find_week_dates

sys.path.append(os.path.join(dirname(dirname(abspath(__file__))), "python templates"))
REPORT_FOLDER = os.path.join(dirname(dirname(dirname(abspath(__file__)))), "reports", "Weekly meeting")


def read_data(filepath):
	with open(filepath, 'rb') as f:
		df = pd.read_csv(f)
	return df


def get_deliveries(dates):
	deliveries = []
	for date_str in dates:
		url = 'https://app.detrack.com/api/v1/deliveries/view/all.json?key=c4a1e371472c7ae6269bba0a4c3b71a76f58b9fd11682ee5&json={"date":"' + date_str +'"}'
		url_hds='https://app.detrack.com/api/v1/deliveries/view/all.json?key=dbcb6f6b1063534fd9eb4ff317635a695b0ae8398060c01e&json={"date":"' + "2018-04-22" +'"}'
		req = requests.post(url_hds)
		print(req)
		contents = json.loads(req.text)
		deliveries += contents["deliveries"]
		break
	return pd.DataFrame(deliveries)


def aggregate_data_from_detrack(year, week):
	output_path = os.path.join(REPORT_FOLDER, f"detrack_week{week:02d}.xlsx")
	dates = find_week_dates(year, week)
	database = get_deliveries(dates)
	return database


def aggregate_data_from_workwave(year, week):
	output_path = os.path.join(REPORT_FOLDER, f"from_ww_week{week:02d}.xlsx")
	database_list = []
	for state in ["NSW", "VIC", "QLD", "ACT"]:
		for f in os.listdir(os.path.join(DROPBOX_FOLDER, f"Weeks {year}", f"Week {week:02d}", state, "03 - From WW")):
			if f.startswith("From WW"):
				try:
					df = pd.read_excel(os.path.join(DROPBOX_FOLDER, f"Weeks {year}", f"Week {week:02d}", state, "03 - From WW", f), sheet_name="Sheet0")
					df = df[df["Type"] == "delivery"]
					db = df[["Vehicle", "Date", "Step Number",
							 "Stop Number", "Current Schedule",
							 "Planned Schedule", "Type", "Name",
							 "Address", "Latitude", "Longitude",
							 "Distance to Next Km", "Drive Time to Next",
							 "Distance Km Left", "Tot Working Time",
							 "Driving Time Left", "Tot Service Time",
							 "Tot Stops", "Tot Orders", "Notes",
							 "Service Time", "Load load",
							 "client"]]
					database_list.append(db)
				except:
					pass
		database = pd.concat(database_list).drop_duplicates()
	return database


def export_data(week, year):
	detrack = aggregate_data_from_detrack(year, week)
	ww = aggregate_data_from_workwave(year, week)
	detrack.to_excel(f"Detrack -  Week {week}.xlsx", index=False)
	ww.to_excel(f"From WW -  Week {week}.xlsx", index=False)


def add_timeslot_bounds(df):
	begin, end = [], []
	for timeslot in df['time_slot']:
		try:
			begin.append(timeslot[:5])
		except:
			begin.append(None)

		try:
			end.append(timeslot[-5:])
		except:
			end.append(None)
	df['Time slot begin'] = begin
	df['Time slot end'] = end


def add_within_timeslot(df):
	within_timeslot = []
	for ind in df.index:
		pod = df['pod_date_time'][ind]
		if pod == "":
			within_timeslot.append(None)
			continue
		if df['Time slot begin'][ind] is None or df['Time slot end'][ind] is None:
			within_timeslot.append(None)
			continue
		pod_ = pod.split(' ')
		hour, minute = int(pod_[1].split(":")[0]), int(pod_[1].split(":")[1])
		if pod_[2] == "PM" and hour < 12:
			hour += 12
		elif pod_[2] == "AM" and hour == 12:
			hour = 0
		pod_time = f"{hour:02d}:{minute:02d}"
		if pod_time >= df['Time slot begin'][ind] and pod_time <= df['Time slot end'][ind]:
			within_timeslot.append(True)
		elif pod_time < df['Time slot begin'][ind]:
			within_timeslot.append('Early')
		elif pod_time > df['Time slot end'][ind]:
			within_timeslot.append('Late')

	df['Within timeslot'] = within_timeslot


def add_recorded_temperature(df):
	recorded_temperature = []
	for temp in df['temp']:
		if temp == "":
			recorded_temperature.append(False)
		else:
			recorded_temperature.append(True)

	df['Recorded temperature'] = recorded_temperature


def add_recorded_POD(df, labels=['p1_at', 
								 'p2_at',
								 'p3_at',
								 'signed_at']):
	recorded_POD = []
	for ind in df.index:
		PODs = 0
		for label in labels:
			if df[label][ind] != "":
				PODs += 1
		if PODs >= 2:
			recorded_POD.append(True)
		else:
			recorded_POD.append(False)

	df['Recorded POD'] = recorded_POD


def build_stats(df):
	counts_per_driver = defaultdict(lambda: {
		'Arrived early': 0,
		'Arrived late': 0,
		'Within timeslot': 0,
		'Did not record POD': 0,
		'Recorded POD': 0,
		'Did not record temperature': 0,
		'Recorded temperature': 0,
		'Something wrong': 0,
		'All good': 0,
		'# of drops': 0,
	})

	counts_per_vehicle = defaultdict(lambda: {
		'Arrived early': 0,
		'Arrived late': 0,
		'Within timeslot': 0,
		'Did not record POD': 0,
		'Recorded POD': 0,
		'Did not record temperature': 0,
		'Recorded temperature': 0,
		'Something wrong': 0,
		'All good': 0,
		'# of drops': 0,
	})

	for ind in df.index:
		if df['group_name'][ind] != "":
			driver = df['assign_to'][ind]
			vehicle = df['run_no'][ind]

			timeslot = df['Within timeslot'][ind]
			counts_per_driver[driver]['# of drops'] += 1
			counts_per_vehicle[vehicle]['# of drops'] += 1
			if timeslot == True:
				counts_per_driver[driver]['Within timeslot'] += 1
				counts_per_vehicle[vehicle]['Within timeslot'] += 1
			elif timeslot == 'Early':
				counts_per_driver[driver]['Arrived early'] += 1
				counts_per_vehicle[vehicle]['Arrived early'] += 1
			elif timeslot == 'Late':
				counts_per_driver[driver]['Arrived late'] += 1
				counts_per_vehicle[vehicle]['Arrived late'] += 1

			temperature = df['Recorded temperature'][ind]
			if temperature == True:
				counts_per_driver[driver]['Recorded temperature'] += 1
				counts_per_vehicle[vehicle]['Recorded temperature'] += 1
			elif temperature == False:
				counts_per_driver[driver]['Did not record temperature'] += 1
				counts_per_vehicle[vehicle]['Did not record temperature'] += 1

			POD = df['Recorded POD'][ind]
			if POD == True:
				counts_per_driver[driver]['Recorded POD'] += 1
				counts_per_vehicle[vehicle]['Recorded POD'] += 1
			elif POD == False:
				counts_per_driver[driver]['Did not record POD'] += 1
				counts_per_vehicle[vehicle]['Did not record POD'] += 1

			if timeslot == "" or temperature == "" or POD == "" is None:
				continue

			if timeslot == True and temperature == True and POD == True:
				counts_per_driver[driver]['All good'] += 1
				counts_per_vehicle[vehicle]['All good'] += 1
			else:
				counts_per_driver[driver]['Something wrong'] += 1
				counts_per_vehicle[vehicle]['Something wrong'] += 1

	return counts_per_vehicle, counts_per_driver


def export_stats(counts, output_file_path):
	with open(output_file_path, 'w') as output:
		header = ['', 'Arrived early', 'Arrived late', 'Within timeslot',
				  'Did not record temperature', 'Recorded temperature',
				  'Did not record POD', 'Recorded POD',
				  'Something wrong', 'All good', '# of drops']
		
		output.write(",".join(header) + "\n")

		for ind, stat in counts.items():
			try:
				timeslot_ratio = (stat['Within timeslot'] / (stat['Within timeslot'] + stat['Arrived early'] + stat['Arrived late'])) * 100
				early_ratio = (stat['Arrived early'] / (stat['Within timeslot'] + stat['Arrived early'] + stat['Arrived late'])) * 100
				late_ratio = (stat['Arrived late'] / (stat['Within timeslot'] + stat['Arrived early'] + stat['Arrived late'])) * 100
			except:
				timeslot_ratio = 0
				early_ratio = 0
				late_ratio = 0
			try:
				temperature_ratio = (stat['Recorded temperature'] / (stat['Recorded temperature'] + stat['Did not record temperature'])) * 100
			except:
				temperature_ratio = 0
			try:
				POD_ratio = (stat['Recorded POD'] / (stat['Recorded POD'] + stat['Did not record POD'])) * 100
			except:
				POD_ratio = 0
			try:
				all_good_ratio = (stat['All good'] / (stat['All good'] + stat['Something wrong'])) * 100
			except:
				all_good_ratio = 0

			# row_to_write = [ind,
			# 				str(early_ratio), str(late_ratio), str(timeslot_ratio),
			# 				str(100 - temperature_ratio), str(temperature_ratio),
			# 				str(100 - POD_ratio), str(POD_ratio),
			# 				str(100 - all_good_ratio), str(all_good_ratio)]
			row_to_write = [ind,
				str(stat['Arrived early']), str(stat['Arrived late']), str(stat['Within timeslot']),
				str(stat['Did not record temperature']), str(stat['Recorded temperature']),
				str(stat['Did not record POD']), str(stat['Recorded POD']),
				str(stat['Something wrong']), str(stat['All good']),
				str(stat['# of drops'])
			]

			output.write(",".join(row_to_write) + "\n")


if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('-t','--times', nargs='+', help='Specify dates for which to download deliveries from Detrack with str format. Ex: 2018-02-15',
						required=True)
	parser.add_argument("-y", "--year", type=int, required=True)
	parser.add_argument("-w", "--week", type=int, required=True)
	args = parser.parse_args()

	dates = args.times
	year = args.year
	week = args.week
	driver_output_file_path = os.path.join(REPORT_FOLDER, f"Weekly Meeting {year}", f"WK {week:02d}", f"drivers_week{week:02d}.csv")
	vehicle_output_file_path = os.path.join(REPORT_FOLDER, f"Weekly Meeting {year}", f"WK {week:02d}", f"vehicles_week{week:02d}.csv")

	df = get_deliveries(dates)

	add_timeslot_bounds(df)
	add_within_timeslot(df)
	add_recorded_temperature(df)
	add_recorded_POD(df)

	counts_per_vehicle, counts_per_driver = build_stats(df)
	export_stats(counts_per_vehicle, vehicle_output_file_path)
	export_stats(counts_per_driver, driver_output_file_path)
