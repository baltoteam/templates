import os
import pandas as pd

from collections import defaultdict
from os import listdir
from os.path import abspath, dirname


def split_address(df):
    address_list = [address.split(",") for address in df["Address"]]
    splitted_addresses = {"street":[], "suburb":[], "state":[], "postcode":[]}
    for address in address_list:
        try:
            splitted_addresses["postcode"].append(int(address[-1]))
            splitted_addresses["suburb"].append(address[-3])
            splitted_addresses["state"].append(address[-2])
            if len(address) == 4:
                splitted_addresses["street"].append(address[0])
            elif len(address) == 5:
                splitted_addresses["street"].append(f"{address[0], address[1]}")
        except:
            if address[-1].lower().replace(" ", "") == "australia":
                try:
                    splitted_addresses["street"].append(address[0])
                    splitted_addresses["suburb"].append(address[1].split(" ")[0])
                    splitted_addresses["state"].append(address[1].split(" ")[1])
                    splitted_addresses["postcode"].append(address[1].split(" ")[2])
                except:
                    pass
    return splitted_addresses


def wrong_line(df):
    keep = []
    for suburb in df["Suburb"]:
        starts_with_num = False
        for num in range(10):
            if suburb.startswith(str(num)):
                starts_with_num = True
        if suburb == "" or suburb.lower().startswith("unit") or starts_with_num:
            keep.append(False)
        else:
            keep.append(True)
    return keep


def make_suburb_column(df):
    sub = []
    address_list = []
    for address in df["Address"]:
        try:
            address_list.append(address.split(","))
        except AttributeError:
            address_list.append([""])
    for address in address_list:
        try:
            sub.append(address[-3].lower().lstrip(" "))
        except:
            if address[-1].lower().replace(" ", "") == "australia":
                try:
                    sub.append(address[1].split(" ")[0])
                except:
                    sub.append("")
            else:
                sub.append("")
    df["Suburb"] = sub
    df["Keep"] = wrong_line(df)
    df = df[df["Keep"] == True]
    del df["Keep"]
    del df["Address"]
    return df


def make_postcode_column(df):
    postcodes = []
    for address in df["Address"]:
        try:
            address_list = address.split(",")
        except AttributeError:
            postcode = None
            postcodes.append(postcode)
            continue
        try:
            postcode = int(address_list[-1])
        except ValueError:
            try:
                postcode = int(address_list[-2])
            except:
                postcode = None
        postcodes.append(postcode)
    return postcodes


def find_drivers_contractor(driver_contractor_file = "driver_contractor.xlsx"):
    # Go get driver - contractor file in the databases folder
    driver_contractor = pd.read_excel(os.path.join("databases", driver_contractor_file))
    driver_dict = defaultdict(str)
    for ind in driver_contractor.index:
        driver = driver_contractor["Driver"][ind]
        contractor = driver_contractor["Contractor"][ind]
        driver_dict[driver] = contractor
    return driver_dict


def get_drivers_for_one_contractor(driver_dict, contractor_name):
    drivers = []
    for driver, contractor in driver_dict.items():
        if contractor == contractor_name:
            drivers.append(driver)
    return drivers


def make_contractor_column(df):
    driver_dict = find_drivers_contractor()
    contractors = []
    for driver in df["Assign to"]:
        contractors.append(driver_dict[driver])
    return contractors


def driver_database_from_last_3_weeks(week, year, states=["NSW", "ACT", "VIC", "QLD"]):
    dflist = []
    for week in range(week - 2, week + 1):
        for state in states:
            try:
                dir_ = f"../Operations & Tech/Weeks {year}/Week {week:02d}/{state}/05 - Detrack"
            except:
                dir_ = f"../Operations & Tech/Weeks {2018}/Week {week:02d}/{state}/06 - Detrack"
            for fname in listdir(dir_):
                try:
                    df = pd.read_excel(f"{dir_}/{fname}")
                    dflist.append(df[["Address", "Assign to"]])
                except:
                    pass
    database = pd.concat(dflist).reset_index()
    database["Postcode"] = make_postcode_column(database)
    database["Contractor"] = make_contractor_column(database)
    database = database.dropna(0, how='any', subset=["Postcode", "Assign to"])
    del database["Address"]
    return database


def postcode_dict(database):
    driver_to_contractor = find_drivers_contractor()
    # Stores how many times each postcode was visited by each driver
    # ex:
    # 2214: Massi NSW: 3
    #       Douglas NSW: 5
    # etc...
    contractor_dict = defaultdict(lambda: defaultdict(int))
    driver_dict = defaultdict(lambda: defaultdict(int))
    for ind in database.index:
        postcode = int(database["Postcode"][ind])
        driver = database["Assign to"][ind]
        contractor = driver_to_contractor[driver]
        if contractor == "":
            continue
        else:
            contractor_dict[postcode][contractor] += 1
            driver_dict[postcode][driver] += 1
    return contractor_dict, driver_dict


def get_top_contractors_per_postcode(contractor_dict, postcode):
    top_contractors = sorted(contractor_dict[postcode].items(), key=lambda x: x[1])
    return top_contractors


def get_sorted_contractors(df, contractor_dict):
    sorted_contractors_per_run = defaultdict(tuple)
    contractor_number_per_run = defaultdict(lambda: defaultdict(int))
    for ind in df.index:
        vehicle = df["Vehicle"][ind]
        postcode = df["Postcode"][ind]
        contractors = get_top_contractors_per_postcode(contractor_dict, postcode)
        for contractor in contractors:
            contractor_number_per_run[vehicle][contractor[0]] += 1

    for vehicle, contractor_number_dict in contractor_number_per_run.items():
        sorted_contractors = sorted(contractor_number_dict.items(), key=lambda x: x[1], reverse=True)
        sorted_contractors_per_run[vehicle] = sorted_contractors
    return sorted_contractors_per_run


def get_top_contractors_per_run(df, contractor_dict):
    sorted_contractors_per_run = get_sorted_contractors(df, contractor_dict)
    top_contractors_per_run = defaultdict(tuple)
    for vehicle, sorted_contractors in sorted_contractors_per_run.items():
        top_contractors = []
        top_score = sorted_contractors[0][1]
        for contractor in sorted_contractors:
            if contractor[1] == top_score:
                top_contractors.append(contractor[0])
            else:
                break
        top_contractors_per_run[vehicle] = top_contractors
    return top_contractors_per_run


def filter_driver_dict(sorted_drivers, available_drivers):
    for driver in sorted_drivers:
        if driver[0] not in available_drivers:
            sorted_drivers.remove(driver)
    return sorted_drivers


def get_top_drivers_per_run(df, driver_dict, top_contractors_per_run):
    sorted_drivers_per_run = get_sorted_contractors(df, driver_dict)
    top_drivers_per_run = defaultdict(set)
    for vehicle, top_contractors in top_contractors_per_run.items():
        available_drivers = []
        sorted_drivers = sorted_drivers_per_run[vehicle]
        for contractor in top_contractors:
            available_drivers += get_drivers_for_one_contractor(find_drivers_contractor(), contractor)
        scored_available_drivers = filter_driver_dict(sorted_drivers, set(available_drivers))

        top_drivers = [driver[0] for driver in scored_available_drivers[:3]]
        top_drivers_per_run[vehicle] = top_drivers
    return top_drivers_per_run


def assign_drivers_and_contractors(df, week, year):
    database = driver_database_from_last_3_weeks(week, year)
    contractor_dict, driver_dict = postcode_dict(database)
    df["Postcode"] = make_postcode_column(df)

    top_contractors_per_run = get_top_contractors_per_run(df, contractor_dict)
    top_drivers_per_run = get_top_drivers_per_run(df, driver_dict, top_contractors_per_run)

    df["Contractor"] = [", ".join(top_contractors_per_run[vehicle]) for vehicle in df["Vehicle"]]
    df["Driver"] = [", ".join(top_drivers_per_run[vehicle]) for vehicle in df["Vehicle"]]
    return df
