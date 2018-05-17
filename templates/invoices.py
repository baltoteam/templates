import pandas as pd
import os
import sys

from collections import defaultdict

sys.path.append(os.path.join(dirname(dirname(abspath(__file__))), "python templates"))


def import_runs(week_tab, list_tab):
	# Computes the cost of each run
	# For each contractor, assign a list of:
	# Contractor; Payment type; Route Code; Company; Day; Location/type; Driver; Drops; Price; Km surcharge; Outcomes; GST; Due
	run_dict = defaultdict(lambda: {"runs": [],
									"total": 0,
									"total_with_gst": 0})
	df = pd.read_csv(week_tab, sep=",")
	for ind in df.index:
		contractor = df["Contractors"][ind]
		payment_type = df["Payment type"][ind]
		route_code = df["Route Code"][ind]
		company = df["Company"][ind]
		day = df["Day"][ind]
		location = df["Location"][ind]
		driver = df["Driver"][ind]
		drops = 1
		price = df["Price"][ind]
		km_surcharge = df["Km Surcharge"][ind]
		outcomes = int(price) + int(km_surcharge)
		gst = gst_per_contractor[contractor]
		due = outcomes * (1 + float(gst))

		row = [contractor, payment_type, route_code,
			   company, day, location, driver, drops,
			   price, km_surcharge, outcomes, gst, due]
		run_dict[contractor]["runs"].append(row)
		run_dict[contractor]["total"] += outcomes
		run_dict[contractor]["total_with_gst"] += due
	return run_dict



def import_errors(error_tab):
	# Adds costs for errors made by drivers
	error_dict = defaultdict(lambda: defaultdict(lambda:defaultdict()))
	df = pd.read_csv(error_tab, sep=",")
	for ind in df.index:
		contractor = df["Contractor"][ind]
		err_type = df["Error type"][ind]
		cost = df["Compensation"][ind]
		error_dict[contractor][err_type]["Cost"] = cost
		if err_type not in error_dict[contractor].keys():
			error_dict[contractor][err_type]["Number"] = 1
		else:
			error_dict[contractor][err_type]["Number"] += 1

	return error_dict


def import_extra(price_and_extra):
	# Adds extra costs for each contractors
	extra_dict = defaultdict(lambda: {"extra": [],
									  "total": 0,
									  "total_with_gst": 0})
	df = pd.read_csv(price_and_extra, sep=",")
	for ind in df.index:
		type_ = df["Type"][ind]
		reason = df["Reason"][ind]
		company = df["Company"][ind]
		amount = df["Amount"][ind]
		qty = 1
		gst = gst_per_contractor(df["Contractor"][ind])
		description = f"{reason} - {company}"
		extra_dict[df["Contractor"][ind]]["extra"].append([type_, description, amount, qty, amount * qty,
												  gst, amount * qty * gst])
		extra_dict[df["Contractor"][ind]]["total"] += amount * qty
		extra_dict[df["Contractor"][ind]]["total_with_gst"] += amount * qty * gst

	return extra_dict


# def contractor_info(extra_list, insurance, insurance_price, public_liability_price):
# 	# Fixes fees for insurance and public liabilities
# 	info_dict = defaultdict(int)
# 	df = pd.read_csv(insurance, sep=",")
# 	for ind in df.index:
# 		cost = 0
# 		contractor = df["Contractor"][ind]
# 		drivers = df["Drivers"][ind]
# 		if df["Insurance"] == "No":
# 			cost -= insurance_price * drivers
# 		if df["Public Liability"] == "No":
# 			cost -= public_liability_price * drivers

# 		info_dict[contractor] = cost

# 	return info_dict


def make_invoice(invoice_file, contractor, runs, errors, contractor_info, extra):
	with open(invoice_file, "w") as output:
		# runs
		header = ["Contractor", "Payment type", "Route Code",
			  	  "Company", "Day", "Location / type", "Driver",
			  	  "Drops", "Price", "Km Surcharge", "Outcomes",
			  	  "GST", "Due\n"]
		output.write(",".join(header))

		for run in runs[contractor]:
			output.write(",".join(run) + "\n")

		output.write(",".join(["Total deliveries",
							   "", "", "",
							   runs[contractor]["total"],
			 				   runs[contractor]["total_with_gst"],
							   "\n"]))

		# errors
		output.write(",".join(["Payback - mistakes", "", "",
							   "Costs", "GST", "Due\n"]))
		total_errors = 0.
		gst = float(gst_per_contractor[contractor])
		for error_type, error in errors[contractor].items():
			total_cost = int(error["Number"]) * float(error["Cost"])
			total_cost_with_gst = total_cost * (1 + gst)
			total_errors += total_cost
			output.write(",".join([error_type, error["Number"], error["Cost"],
								   total_cost, gst,
								   total_cost_with_gst] + "\n"))


		output.write(",".join(["Total errors", "", "", total_errors, gst,
							   total_errors * (1 + gst)] + "\n"))

		# extra
		for ex in extra[contractor]["extra"]:
			output.write(",".join(ex) + "\n")
		output.write(",".join(["Additional fees", "", "", "", extra[contractor]["total"],
								gst, extra[contractor]["total_with_gst"]] + "\n"))

		# sum up

		output.write(f"Total deliveries, {runs[contractor]['total_with_gst']}\n")
		output.write(f"Total errors, {total_errors * (1 + gst)}\n")
		output.write(f"Additional fees, {extra[contractor]['total_with_gst']}\n")

		full_total = runs[contractor]['total_with_gst'] + total_errors * (1 + gst) + extra[contractor]['total_with_gst'] 
		output.write(f"Total, {full_total}\n")


def main(week_tab, list_tab, error_tab, price_and_extra):
	  import_runs(week_tab, list_tab)
