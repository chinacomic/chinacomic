#!/usr/bin/env python

import os,sys,re,time,datetime,psycopg2,psycopg2.extras
import datetime
from collections import defaultdict


##January 30, 2018: I adjusted the history scans so that we will not get false negatives on parkers with multiple names.  We were missing accurate history for some people.


park_record = []

cardscans = defaultdict(list)

try:
    conn = psycopg2.connect("host='localhost' dbname='someDB' user='xxxxxx' password='xxxxx'")
except:
    print("Unable to connect to the database")
    sys.exit(1)
curr = conn.cursor()

###The next couple of lines reset the parking table in preparation for the current month
###First delete the already marked, dropped names from last month
curr.execute("""DELETE FROM tracking_parking WHERE Status='Delete'""")
conn.commit()

###Next, change the status of the "new" cardkey holders from last month to a "no change" status
curr.execute("""UPDATE tracking_parking SET Status='No Change' WHERE Status='New'""")
conn.commit()

###Names of companies
with open("COMPANY") as fop:
    content = fop.read()

###Individual cardholder names
with open("NAMES") as fp:
    contents = fp.read()

###Cardkey numbers and access data information
with open("CARDS") as fp1:
    contents1 = fp1.read()

###Codes corresponding to access group levels
with open("AclGrpCombo") as fp2:
    contents2 = fp2.read()

###Names of access groups (like "Parking" or "Reserved Parking")
with open("AclGrpName") as fp3:
    contents3 = fp3.read()

###Cardkey historical logs
with open("file.csv") as fp4:
    contents4 = fp4.read()

###This opens the first file for saving the overall report
ctoday = datetime.date.today()
currdate = ctoday.strftime('%b%d-%y')
output_file1 = currdate + "Non-Reserved"
of1 = open(output_file1, 'a')
output_file2 = currdate + "Reserved"
of2 = open(output_file2, 'a')

###This opens a second file to gather the drops list for my removal from the database later
drops_file = "droplist.txt"
of3 = open(drops_file, 'a')

tenant_totals = []
card_drops = []

###Search and begin parsing the list of names of companies
regfop = re.compile(r"-{,1}\d+,1,(\d{1,2}),\"([A-Za-z0-9\ &/]+)\"")
matfop = regfop.findall(content)
for i in matfop:
    pctr = 0
    rpctr = 0
    prec1 = []
    ###Return an introduction line showing the company currently being searched
    company_name = str(i[1])
    of1.write("The following is for %s\n\n" % company_name)
    of2.write("The following is for %s\n\n" % company_name)
    print("\n\nCurrently working on data for %s" % company_name)
    time.sleep(3)
    curr.execute("""SELECT id FROM companies WHERE companies.bname=%s""", [company_name])
    rows = curr.fetchall()
    if not rows:
        ##This piece will add the company if it is not found in the database
        curr.execute("""INSERT INTO companies (bname) VALUES (%s)""", [company_name])
        conn.commit()
        print("\n***********************  NEW COMPANY - %s  *****************************\n\n" % company_name)
        curr.execute("""SELECT id FROM companies WHERE companies.bname=%s""", [company_name])
        rows = curr.fetchall()
    ###This will get the corresponding id # for the company to find the contact info on this tenant
    for z in rows:
        company_id = z[0]
        #print("%d is the company id number for %s" % (company_id,company_name))
        curr.execute("""SELECT id,name FROM contacts WHERE company=%s""", ([company_id]))
        rows2 = curr.fetchall()
        ###This clause will either get the contact information for the given company (including the contact ID), or it will add a fake contact
        if not rows2:
            print("No data was available for this tenant; adding a placeholder tenant contact\n\n")
            curr.execute("""INSERT INTO contacts (company,name,private,associated_user,is_active) VALUES (%s,%s,%s,%s,%s)""",(company_id,"John Doe",True,36,True))
            conn.commit()
            curr.execute("""SELECT id,name FROM contacts WHERE company=%s""", ([company_id]))
            rows2 = curr.fetchall()
        for y in rows2:
            contact_id = y[0]
            #print("%d is the contact number for %s" % (contact_id,company_name))
            curr.execute("""SELECT id,tname FROM tenants WHERE tname=%s""", [company_name])
            rows3 = curr.fetchone()
            if rows3:
                tenant_company_id = rows3[0]
                #print("%s is the tenant id for %s" % (tenant_company_id,company_name))

            else:
                print("%s is a new tenant who has just now been added to the database\n\n" % company_name)
                curr.execute("""INSERT INTO tenants (tname,phone,published) VALUES (%s,%s,%s)""",(company_name,"555-555-5555",'f'))
                conn.commit()
                curr.execute("""SELECT id,tname FROM tenants WHERE tname=%s""", [company_name])
                rows3 = curr.fetchone()
                tenant_company_id = rows3[0]

    ###Set up dictionaries to keep track of new, existing and total parkers for this tenant
    recpark = []
    new_adds_r = {}
    new_adds = {}
    existing_r = {}
    existing = {}
    total_in_cksys = {}
    total_in_cksys_r = {}
    total_in_db = {}
    total_in_db_r = {}
    drops = {}
    
    curr.execute("""SELECT card_number,parker_name FROM tracking_parking WHERE tname=%s AND space_type='R'""" % str(tenant_company_id))
    row6 = curr.fetchall()
    if row6:
        for w in row6:
            #print("The database lists %s %s" % (str(w[0]), w[1]))
            total_in_db_r[str(w[0])] = w[1]

    curr.execute("""SELECT card_number,parker_name FROM tracking_parking WHERE tname=%s AND space_type='N'""" % str(tenant_company_id))
    row7 = curr.fetchall()
    if row7:
        for x in row7:
            #print("The database lists %s %s" % (str(x[0]), x[1]))
            total_in_db[str(x[0])] = x[1]
    
    ###Use the number corresponding to this tenant to search the names list and find all the tenants belonging to this company
    ###Remember, this is all the tenants that have cardkeys - not just parkers
    tnnum = i[0]
    regex = re.compile(
        r"(-{,1}\d{6,10}),\d{1},\"([A-Za-z\ .'`-]+)\",\"([A-Za-z\ .'`-]+)\",%s,\d{1},\d{1},\d{1}" % tnnum)
    match = regex.findall(contents)
    ###Use the numbers corresponding to the tenant names to find the cardkey numbers for each tenant
    for k in match:
        zndr = str(k[0])

        print("%s" % zndr)
        
        first_name = k[2]
        last_name = k[1]
        print("%s %s - %s" % (first_name, last_name, zndr))
        regex1 = re.compile(r"-{,1}\d+,%s,1,(\d{1}\.\d+[e][+]\d{2}),\d+,\"(\d{2}/\d{2}/\d{2})\ \d{2}:\d{2}:\d{2}\",\"\d{2}/\d{2}/\d{2}\ \d{2}:\d{2}:\d{2}\",1,\"(\d{3,6})\",0,9999,\"{,1}[A-Za-z-\ ]*\"{,2},0,(-{,1}\d+)," % zndr)
        match1 = regex1.findall(contents1)
        ###Use the cardkey number to find the group access name(s) assigned to this card
        for j in match1:

            #print("Match - %s" %  str(j))
            ###This was done to deal with the scientific notation problem that was discovered.  We convert the file.csv file first with awk now to get the information into a more helpful format.
            specialj = float(j[0])
            specjprint = int(specialj)
            cardkey_no = j[2]
            #noteworthy = j[3]
            activation_date = j[1]
            regex2 = re.compile(r"-{,1}\d+,(-{,1}\d+),%s," % j[3])
            match2 = regex2.findall(contents2)
            ###Check the log files to see when the last time that this card was used
            #ADDED BELOW LINE TO FIX FOR CORNER CASES WHERE THE LAST SCAN WAS LAST YEAR; USEFUL IN JANUARY 
            regex4 = re.compile(
                r"\"(\d{2}/\d{2}/\d{2})\ \d{2}:\d{2}:\d{2}\",\d+,\d+,\d+,\d+,\"Park[a-zA-Z0-9\ ]+\",%s,\"[A-Za-z\ .'`-]+\",\"[A-Za-z\ .'`-]+\"," % (specjprint))
            match4 = regex4.findall(contents4)
            ###Print results of search for usage of card
            if match4:
                #last_parked = datetime.datetime.strptime(match4[-1][0],'%m/%d/%y').strftime('%B %d')
                for lastp in match4:
                    #recpark.append(lastp)
                    cardscans[cardkey_no].append(lastp)
                cardscans[cardkey_no].sort()
            #else:
                #last_parked = "***No results***"
                #last_parked = "NA"
            if len(cardscans[cardkey_no]) > 0:
                cardscans[cardkey_no].sort()
                noscans = len(cardscans[cardkey_no])
                last_parked = cardscans[cardkey_no][-1]
                #print(last_parked)
                #print("%s times parked" % noscans)
            else:
                last_parked = "***No results***"
                noscans = 0
            for l in match2:
                ###Look for the names of the access levels and match for Parking or Reserved Parking
                regex3 = re.compile(r"%s,\d,\"(\w+\ {,1}\w+)" % l)
                match3 = regex3.findall(contents3)
                for m in match3:
                    ###This will pull out the specific type of parker, whether reserved or just parking enabled
                    ###Below consolidates the first and last name for entry into the database
                    full_name = first_name + " " + last_name
                    ###The SQL statement below checks the database for the given tenant / parker / cardkey combo
                    curr.execute("""SELECT parker_name,card_number FROM tracking_parking WHERE tname=%s AND parker_name=%s AND card_number=%s""",(str(tenant_company_id),full_name,cardkey_no))
                    row5 = curr.fetchone()
                    if m == "Reserved Parking":
                        rpctr += 1
                        parking_descriptor = "R"
                        #prec1.append([first_name,last_name,cardkey_no,noscans,last_parked,parking_descriptor])
                        if row5:
                            existing_r[cardkey_no] = full_name
                            prec1.append([first_name,last_name,cardkey_no,noscans,last_parked,parking_descriptor])
                            #print("%s is Reserved but existing" % full_name)
                        else:
                            ###Adding "new add" to the dictionary list for reference
                            new_adds_r[cardkey_no] = full_name
                            orig_full = first_name + " " + last_name
                            first_name = "*" + first_name
                            full_name = first_name + " " + last_name
                            prec1.append([first_name,last_name,cardkey_no,noscans,last_parked,parking_descriptor])
                            #print("%s is Reserved but a new add" % full_name)
                            ###The insert statement below will insert the parker into the parking database, complete with cardkey number and parking type
                            curr.execute("""INSERT INTO tracking_parking (tname,parker_name,space_type,card_number,entered,bill_type,status) VALUES (%s,%s,%s,%s,%s,%s,%s)""",(tenant_company_id,orig_full,parking_descriptor,cardkey_no,datetime.datetime.now(),"Other","New"))
                            conn.commit()
                    elif m == "Parking":
                        pctr += 1
                        parking_descriptor = "N"
                        #prec1.append([first_name,last_name,cardkey_no,noscans,last_parked,parking_descriptor])
                        if row5:
                            existing[cardkey_no] = full_name
                            prec1.append([first_name,last_name,cardkey_no,noscans,last_parked,parking_descriptor])
                            #print("%s is Non-Reserved but existing" % full_name)
                        else:
                            ###Adding "new add" to the dictionary list for reference
                            new_adds[cardkey_no] = full_name
                            orig_name = first_name
                            orig_full = orig_name + " " + last_name
                            first_name = "*" + first_name
                            full_name = first_name + " " + last_name
                            prec1.append([first_name,last_name,cardkey_no,noscans,last_parked,parking_descriptor])
                            ###The insert statement below will insert the parker into the parking database, complete with cardkey number and parking type
                            curr.execute("""INSERT INTO tracking_parking (tname,parker_name,space_type,card_number,entered,bill_type,status) VALUES (%s,%s,%s,%s,%s,%s,%s)""",(tenant_company_id,orig_full,parking_descriptor,cardkey_no,datetime.datetime.now(),"Other","New"))
                            conn.commit()
                        ###Collect the data regarding each parker to be sorted alphabetically
                    else:
                        #print("I got the value of %s for %s" % (m, full_name))
                        pass
    park_record = sorted(prec1, key=lambda lname: lname[1])
    for lc in park_record:
        lon = len(lc[0]) + len(lc[1])
        if lc[5] == "N":
            if lon <= 7:
                of1.write("NAME: %s %s \t\t\tCARDKEY: %s\tNS: %s\tLDP: %s\n" % (lc[0],lc[1],lc[2],lc[3],lc[4]))
            elif lon >= 16:
                of1.write("NAME: %s %s \tCARDKEY: %s\tNS: %s\tLDP: %s\n" % (lc[0],lc[1],lc[2],lc[3],lc[4]))
            elif lon >= 23:
                of1.write("NAME: %s %s CARDKEY: %s\tNS: %s\tLDP: %s\n" % (lc[0],lc[1],lc[2],lc[3],lc[4]))
            else:
                of1.write("NAME: %s %s \t\tCARDKEY: %s\tNS: %s\tLDP: %s\n" % (lc[0],lc[1],lc[2],lc[3],lc[4]))
        elif lc[5] == "R":
            if lon <= 7:
                of2.write("NAME: %s %s \t\t\tCARDKEY: %s\tNS: %s\tLDP: %s\n" % (lc[0],lc[1],lc[2],lc[3],lc[4]))
            elif lon >= 16:
                of2.write("NAME: %s %s \tCARDKEY: %s\tNS: %s\tLDP: %s\n" % (lc[0],lc[1],lc[2],lc[3],lc[4]))
            elif lon >= 23:
                of2.write("NAME: %s %s CARDKEY: %s\tNS: %s\tLDP: %s\n" % (lc[0],lc[1],lc[2],lc[3],lc[4]))
            else:
                of2.write("NAME: %s %s \t\tCARDKEY: %s\tNS: %s\tLDP: %s\n" % (lc[0],lc[1],lc[2],lc[3],lc[4]))
    of1.write("\n***************CHANGES*************************\n")
    of2.write("\n***************CHANGES*************************\n")
    of1.write("\n\t\tADDS\n")
    of2.write("\n\t\tADDS\n")
    of1.write("There were a total of %d new parkers added in the past month\n (These are marked with an asterisk in the list above)\n" % len(new_adds))
    of2.write("There were a total of %d new parkers added in the past month\n (These are marked with an asterisk in the list above)\n" % len(new_adds_r))
    for u in new_adds:
        of1.write("%s %s\n" % (u, new_adds[u]))
    for uu in new_adds_r:
        of2.write("%s %s\n" % (uu, new_adds_r[uu]))
    total_in_cksys_r = existing_r.copy()
    total_in_cksys_r.update(new_adds_r)
    total_in_cksys = existing.copy()
    total_in_cksys.update(new_adds)

    drops_r = set(total_in_db_r) - set(total_in_cksys_r)
    drops = set(total_in_db) - set(total_in_cksys)
    of1.write("\n\t\tDROPS\n")
    of2.write("\n\t\tDROPS\n")
    of1.write("There were a total of %s drops in the past month (listed below)\n\n" % len(drops))
    of2.write("There were a total of %s drops in the past month (listed below)\n\n" % len(drops_r))
    for v in drops:
        #print(v)
        ###Adjust the parkers that have disappeared this month to show that they are delete candidates
        curr.execute("""UPDATE tracking_parking SET Status='Delete' WHERE card_number=%s""" % v)
        conn.commit()
        #time.sleep(5)
        
        of1.write("%s %s\n" % (v, total_in_db[v]))
        #of3.write("%s %s\n" % (v, total_in_db[v]))
        card_drops.append([v, total_in_db[v]])

    for vv in drops_r:
        of2.write("%s %s\n" % (vv, total_in_db_r[vv]))
        ###Adjust the parkers that have disappeared this month to show that they are delete candidates
        curr.execute("""UPDATE tracking_parking SET Status='Delete' WHERE card_number=%s""" % vv)
        conn.commit()
        #of3.write("%s %s\n" % (v, total_in_db[v]))
        card_drops.append([vv, total_in_db_r[vv]])

    of1.write("\n**************MONTH SUMMARY***********************\n")
    of2.write("\n**************MONTH SUMMARY***********************\n")
    of1.write("\nThe total of Non-Reserved for %s is: %d\n" % (company_name, pctr))
    of2.write("\nThe total of Reserved for %s is: %d\n" % (company_name, rpctr))
    tenant_totals.append([company_name, pctr])
    of1.write("\n-----------------------------------------------------------------------\n")
    of1.write("-----------------------------------------------------------------------\n\n")
    of2.write("\n-----------------------------------------------------------------------\n")
    of2.write("-----------------------------------------------------------------------\n\n")
    ncomp = company_name.rstrip()
    n2comp = ncomp.replace(' ', '-')
    complen = len(n2comp)
    if complen <= 6:
        of3.write("%s \t\t\t\t\t %d \t %d\n" % (n2comp, pctr, rpctr))
    elif complen <= 11:
        of3.write("%s \t\t\t\t %d \t %d\n" % (n2comp, pctr, rpctr))
    elif complen <= 17:
        of3.write("%s \t\t\t %d \t %d\n" % (n2comp, pctr, rpctr))
    elif complen <= 22:
        of3.write("%s \t\t %d \t %d\n" % (n2comp, pctr, rpctr))
    elif complen >= 23:
        of3.write("%s \t %d \t %d\n" % (n2comp, pctr, rpctr))
    else:
        of3.write("%s \t\t\t\t %d \t %d\n" % (n2comp, pctr, rpctr))

overall_total_tenant_parkers = 0

of3.write("\n\nDrops for this period:\n\n")
for q in card_drops:
    of3.write("%s %s\n" % (q[0], q[1]))
of3.write("******************************************************************")
of3.close()
of1.close()
of2.close()
