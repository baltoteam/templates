import argparse
import openpyxl
import os
import sys
import pandas as pd

from collections import defaultdict
from os.path import dirname, abspath
from shutil import copy2, rmtree
from tempfile import TemporaryDirectory

sys.path.append(os.path.join(dirname(dirname(abspath(__file__))), "python templates"))
from utils.parameters import DROPBOX_FOLDER
from gmail.send_email import *

CONTRACTORS_EMAIL_FILE = os.path.join(DROPBOX_FOLDER, "Contractors email addresses.xlsx")

def split_run_list(run_list, week, year, contractors, file_dict, tmp):
	for contractor in contractors:
		# contractor_run_list_path = os.path.join(DROPBOX_FOLDER, f"Weeks {year}", f"Week {week:02d}", "Contractors attachments", contractor, f"{contractor} - Run List.xlsx")
		contractor_run_list_path = os.path.join(tmp, f"{contractor} - Run List.xlsx")
		contractor_run_list = run_list[run_list["Contractors"] == contractor]
		contractor_run_list.to_excel(contractor_run_list_path, index=False)
		file_dict[contractor].append(contractor_run_list_path)

		# adjust column width
		wb = openpyxl.load_workbook(contractor_run_list_path)
		worksheet = wb.active
		for col in worksheet.columns:
			max_length = 0
			column = col[0].column # Get the column name
			for cell in col:
				try: # Necessary to avoid error on empty cells
					if len(str(cell.value)) > max_length:
						max_length = len(cell.value)
				except:
					pass
			adjusted_width = (max_length + 2) * 1.2
			worksheet.column_dimensions[column].width = adjusted_width
		wb.save(contractor_run_list_path)

	return file_dict


def select_driver_manifests(run_list, week, year, contractors, file_dict, tmp):
	# Select runs for each contractor
	contractors_run_dict = defaultdict(set)
	for ind in run_list.index:
		run = run_list["Route Code"][ind]
		contractor = run_list["Contractors"][ind]
		contractors_run_dict[contractor].add(run)

	# Attach driver manifest for each contractor
	for contractor in contractors:
		
		for run in contractors_run_dict[contractor]:
			try:
				state = run.split(" ")[1]
				if state not in ["NSW", "VIC", "QLD", "ACT"]:
					print(f"An error occurred. Please check run {run}.")
					continue
			except:
				continue
			driver_manifest = os.path.join(DROPBOX_FOLDER, f"Weeks {year}", f"Week {week:02d}", state, "04 - Driver Manifest", f"driver manifest - {run}.xlsx")
			try:
				copy2(driver_manifest, os.path.join(tmp, f"driver manifest - {run}.xlsx"))
				file_dict[contractor].append(os.path.join(tmp, f"driver manifest - {run}.xlsx"))
			except:
				pass
	return file_dict


def make_file_dict(week, year, tmp):
	run_list_path = os.path.join(DROPBOX_FOLDER, "Contractors attachments", "run_list.xlsx")
	run_list = pd.read_excel(run_list_path)
	run_list["Comment"] = ["" for i in range(len(run_list))]
	contractors = set(run_list["Contractors"])

	file_dict = defaultdict(list)
	file_dict = split_run_list(run_list, week, year, contractors, file_dict, tmp)
	file_dict = select_driver_manifests(run_list, week, year, contractors, file_dict, tmp)

	return file_dict


def attach_files_for_contractors(file_dict):
	for contractor, files in file_dict.items():
		if files == []:
			continue
		contractor_folder = os.path.join(DROPBOX_FOLDER, "Contractors attachments", contractor)
		if not os.path.exists(contractor_folder):
			os.makedirs(contractor_folder)
		for file_path in files:
			copy2(file_path, os.path.join(contractor_folder, os.path.basename(file_path)))


def clear_folder():
	global_folder = os.path.join(DROPBOX_FOLDER, "Contractors attachments")
	for contractor_folder in os.listdir(global_folder):
		if not contractor_folder == "run_list.xlsx":
			rmtree(os.path.join(global_folder, contractor_folder))


def create_drafts(file_dict, day, message):
	credentials = get_credentials()
	http = credentials.authorize(httplib2.Http())
	service = discovery.build('gmail', 'v1', http=http)
	sender = "customerservice@balto.com.au"
	CONTRACTORS_EMAIL = pd.read_excel(CONTRACTORS_EMAIL_FILE).set_index("Contractor").T
	for contractor, files in file_dict.items():
		if files == []:
			continue
		try:
			email_address = CONTRACTORS_EMAIL[contractor]["Email"]
		except KeyError:
			print(f"{contractor}'s email address is missing in the database.")
			continue
		message_to_contractor = create_Message_with_attachment(sender=sender,
															   to=email_address,
															   subject=f"Run list - {day} - {contractor}",
															   plain_text=message,
															   attached_files=files)
		CreateDraft(service, sender, message_to_contractor)
