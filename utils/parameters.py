import os

from collections import defaultdict
from datetime import datetime
from os.path import join
from os.path import dirname, abspath

DROPBOX_FOLDER = os.path.join(dirname(dirname(dirname(abspath(__file__)))), "Operations & Tech")

API_KEYS = {
	"WorkWave": "5c77552f-34f4-4bac-a2a6-5319156b7d17",
	"Detrack": "c4a1e371472c7ae6269bba0a4c3b71a76f58b9fd11682ee5",
}

TIME_SLOTS_AM = {
	False: "08:00 - 18:00",
	True: "00:00 - 07:00",
}

FROM_WW_HEADER = ["Vehicle", "Driver", "Date", "Step Number",
				  "Stop Number", "Order Number", "Current Schedule",
				  "Planned Vehicle", "Planned Schedule", "Type",
				  "Time In", "Time Out", "Auto Time In", "Auto Time Out",
				  "Status", "Status Time", "Name", "Address", "Latitude", "Longitude", "Distance to Next Km", "Drive Time to Next",
				  "Idle Time Here", "Distance Km Left", "Tot Working Time", "Driving Time Left", "Idle Time Left", "Tot Service Time",
				  "Tot Stops", "Tot Orders", "Notes", "Vehicle Tags",
				  "Required Tags", "Banned Tags", "Service Time",
				  "Driver Note", "Load load", "client", "country",
				  "d.o", "email", "hf twe", "hf tws", "id", "phone",
				  "type", "Time Window Start", "Time Window End"]

###########################
# ROUTING DATES #
###########################
ROUTING_DATE = defaultdict(lambda: defaultdict(str))

# Batch 1
ROUTING_DATE["VIC"]["Sunday AM"] = datetime(2018, 5, 25).date()
ROUTING_DATE["VIC"]["Sunday"] = datetime(2018, 5, 26).date()
ROUTING_DATE["QLD"]["Sunday AM"] = datetime(2018, 5, 27).date()

# Batch 2
ROUTING_DATE["VIC"]["Monday"] = datetime(2018, 5, 28).date()
ROUTING_DATE["VIC"]["Tuesday"] = datetime(2018, 5, 29).date()
ROUTING_DATE["QLD"]["Monday"] = datetime(2018, 5, 30).date()

# Batch 3
ROUTING_DATE["NSW"]["Sunday AM"] = datetime(2018, 5, 31).date()
ROUTING_DATE["NSW"]["Tuesday"] = datetime(2018, 6, 1).date()
ROUTING_DATE["QLD"]["Tuesday"] = datetime(2018, 6, 2).date()

# Batch 4
ROUTING_DATE["NSW"]["Sunday"] = datetime(2018, 6, 3).date()
ROUTING_DATE["NSW"]["Monday"] = datetime(2018, 6, 4).date()
ROUTING_DATE["NSW REG"]["Monday AM"] = datetime(2018, 6, 5).date()
ROUTING_DATE["NSW REG"]["Monday"] = datetime(2018, 6, 5).date()
ROUTING_DATE["ACT"]["Monday AM"] = datetime(2018, 6, 6).date()
ROUTING_DATE["ACT"]["Monday"] = datetime(2018, 6, 6).date()
ROUTING_DATE["NSW"]["Monday AM"] = datetime(2018, 6, 7).date()

###########################
# UPDATE EVERY WEEK
###########################


WAREHOUSES = {
"Brookvale": "14/84 Old Pittwater Rd, Brookvale, 2100, NSW",
"Prospect":	"11/29 Stoddart Road, Prospect, NSW, 2148",
"Rocklea": "44 Cambridge St, Yeerongpilly, QLD, 4105",
"Frozen": "2 Connection Drive, Campbellfield, VIC, 3061",
"Campbellfield": "9 Colbert Road, Campbellfield, VIC, 3061",
"Derrimut":	"333 Fitzgerald Road, Derrimut, VIC, 3030",
"Laverton":	"99 William Angliss Drive, Laverton Northm VIC, 3026",
"Palm Beach": "Palm Beach, NSW, 2108",
"Penrith":	"Penrith, NSW, 2750",
"Homebush":	"21B Richmond Road, HomeBush West, NSW, 2140",
"Milperra":	"90 Ashford Ave, Milperra, NSW, 2214",
"South Granville": "9 Ferndell St, South Granville, NSW, 2142",
"Villawood": "2 Birgmongham, Villawood, NSW, 2163",
"Werribee":	"Werribee, VIC, 3030",
"Brisbane - Morningside JAT": "422 Lytton road, morningside, 4170",
"NC": "106 – 108 Station Road. Seven Hills, NSW, 2147",
"Chefs Palate":	"33 Nancarrow Ave, Ryde, NSW, 2112",
"Sydney Airport": "Sydney NSW 2020",
"Pemulwuy":	"2 A Basalt road, Pemulwuy, 2145, NSW",
"Sydney - HelloFresh": "2 A Basalt road, Pemulwuy, 2145, NSW",
"Laverton":	"99-103 William Angliss Dr., Laverton North VIC 3026",
"Seven Hills": "106-108 statito street, seven hills, NSW, 2147",
"Melbourne - Clayton HDS": "50-54 Clayton Road, Clayton 3168 VIC",
"Sydney - Rosehill HDS": "3-11 Shirley St, Rosehill, NSW, 2142",
"Melbourne - Airport": "Melbourne - Airport",
"Brisbane - Airport": "Brisbane - Airport",
"Melbourne Airport": "Melbourne - Airport",
"Brisbane Airport": "Brisbane - Airport",
"Rosehill": "3-11 Shirley St, Rosehill, NSW, 2142",
"Clayton": "50-54 Clayton Road, Clayton 3168 VIC",
"JAT": "422 Lytton road, morningside, 4170",
}


THR1VE_STATES = {
	"SYD METRO": "NSW",
	"MEL METRO": "VIC",
	"BNE METRO": "QLD",
	"MEL GEELONG": "VIC",
	"QLD GOLDCOAST": "QLD",
	"CBR METRO": "ACT",
	"NEWCASTLE": "NSW",
	"NSW WOLLONGONG": "NSW",
	"SYD REGIONAL": "NSW",
	"NSW CENTRAL": "NSW",
	"SYD SOUTHWEST": "NSW",
}

TO_WW_HEADERS = {
	"HelloFresh": [],
	"Soulara": [],
	"Food Club": [],
	"THR1VE": [],
	"Chef Good": [],
	"Fit Foods Club": [],
	"Workout Meals": [],
	"The Juice Fix": [],
	"OneTable": [],
}


# If new days/states of delivery arrive, just add them here
LIST_OF_DELIVERIES = {
	"NSW": ["Sun NSW", "Sun NSW AM", "Mon NSW",
			"Tue NSW", "Mon NSW REG", "Mon NSW AM"],
	"VIC": ["Sun VIC", "Sun VIC AM", "Mon VIC",
			"Tue VIC", "Thu VIC"],
	"QLD": ["Mon QLD", "Tue QLD", "Thu QLD"],
	"ACT": ["Mon ACT"],
}

["Sun NSW", "Sun NSW AM", "Mon NSW",
					  "Tue NSW", "Mon NSW REG", "Sun VIC",
					  "Sun VIC AM", "Mon VIC", "Tue vIC",
					  "Thu VIC", "Sun QLD AM", "Mon QLD",
					  "Tue QLD", "Thu QLD", "Mon ACT"]


DELIVERY_SCHEDULE = {
	"Thr1ve": {
		"NSW": ["Sunday", "Sunday AM", "Monday"],
		"NSW REG": ["Monday"],
		"VIC": ["Sunday", "Monday", "Tuesday"],
		"QLD": ["Monday"],
		"ACT": ["Monday"]
	}
}


# List of the areas listed in the pick-and-pack file sent from each client.
# These are the names of the different tabs of the excel sheet we fill up
PICK_AND_PACK_ZONES = {"Thr1ve": ["NSW", "NSW-NEWCASTLE", "NSW-CENTRAL",
					   			  "NSW WOLLONGONG", "VIC MEL", "QLD", "QLD-GC",
					   			  "ACT", "VIC - GEELONG"]
}


CONTRACTORS_EMAIL = {
	"Ashok": "ashok_a84@yahoo.co.in",
	"Bill": "bill_dailytransport@hotmail.com",
	"Biniam": "bmengstab16@gmail.com",
	"Caio": "ca_pieza@hotmail.com",
	"Douglas": "carvalhodd@hotmail.com",
	"Harman": "info.sahibtrans@gmail.com",
	"Kiflom": "kiflomfeday1@gmail.com",
	"Muhammad": "nabeel.sialvi@gmail.com",
	"Muktar": "muktarhassen.mh@gmail.com",
	"Shane": "shane@afdls.com",
	"Shiv": "shiv_sharma302@yahoo.com",
	"Vikas": "vikaspatel8306@gmail.com",
	"Yohannes": "hjebunegn@y7mail.com",
	"Kulwinder": "office@dariustransport.com.au",
	"Linda": "linda@priceenterprises.com.au",
	"MFR": "info@mfrcoollogistics.com.au",
	"Redex": "redex.transport@gmail.com",
	"Sam": "sam37503@gmail.com",
	"Sher": "grgsher@gmail.com",
	"Andrew": "andrew87naden@gmail.com",
	"Chris": "chris.phillips1990@hotmail.com",
	"Dave": "dave@cavemankitchen.com.au",
	"Richard":"	mcsrefrigeratedcouriers@gmail.com",
	"Becool": "kayla@becoolcouriers.com.au",
	
}


CONTRACTORS_EMAIL2 = {
	"Ashok": "emma@balto.com.au",
	"Bill": "emma@balto.com.au",
	"Biniam": "emma@balto.com.au",
	"Caio": "emma@balto.com.au",
	"Douglas": "emma@balto.com.au",
	"Harman": "emma@balto.com.au",
	"Kiflom": "emma@balto.com.au",
	"Muhammad": "emma@balto.com.au",
	"Muktar": "emma@balto.com.au",
	"Shane": "emma@balto.com.au",
	"Shiv": "emma@balto.com.au",
	"Vikas": "emma@balto.com.au",
	"Yohannes": "emma@balto.com.au",
	"Kulwinder": "emma@balto.com.au",
	"Linda": "emma@balto.com.au",
	"MFR": "emma@balto.com.au",
	"Redex":"emma@balto.com.au",
	"Sam": "emma@balto.com.au",
	"Sher": "emma@balto.com.au",
	"Andrew": "emma@balto.com.au",
	"Chris": "emma@balto.com.au",
	"Dave": "emma@balto.com.au"
}