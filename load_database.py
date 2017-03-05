# load_database.py
# A Python version of what we were trying to write in C++
# Python version = 2.7 (The version that is on CSIF?)

# Everything from the original schema you sent me is here,
# but I added parameters in a few tables where I thought they might simplify the queries
# You can decide whether to keep them or not, it's not hard to change

# How to run this:
# You will have to create an empty database with name postgres

# Have the "Grades" directory (the one which contains all the csv files)
# in the same directory with this program

# Just type "python load_database.py" into the shell
# It will process all the files that are in "Grades" on its own :)
# This should take 10-15 minutes to run

import psycopg2
import string
import decimal


# Need this library to iterate through all files in a directory
import os

# I don't think I'll need the getopt library, but I'll leave it here for now
import sys, getopt

def main(inputfile):

	# Attempt to open file
	f = open(inputfile, 'r')

	# Tries to create table Course
	cur.execute("CREATE TABLE IF NOT EXISTS Course (cid integer, term integer, subj char(3), crse integer, sec integer, min_units integer, max_units integer);")


	# Tries to create table Instructor
	cur.execute("CREATE TABLE IF NOT EXISTS Instructor (inst varchar(50), type varchar(50), days varchar(10), time varchar(20), build varchar(50), room varchar(50));")
	

	# Tries to create table Student 
	cur.execute("CREATE TABLE IF NOT EXISTS Student (sid integer, surname varchar(50), prefname varchar(50), level varchar(5), units real, class char(2), major char(4), grade varchar(3), status char(2), email varchar(50));")


	# Tries to create table courseNDinst
	cur.execute("CREATE TABLE IF NOT EXISTS courseNDinst (cid integer, term integer, inst varchar(50), type varchar(50), days varchar(10), time varchar(20));")


	# Tries to create table courseNDstud
	cur.execute("CREATE TABLE IF NOT EXISTS courseNDstud (cid integer, term integer, sid integer, seat integer, units real, grade varchar(3));")
	

	# Tries to create table studNDinst
	cur.execute("CREATE TABLE IF NOT EXISTS studNDinst (cid integer, term integer, sid integer, inst varchar(50),  days varchar(10), time varchar(20), grade varchar(3));")	


	# Attempts to read lines from input file
	line = f.readline()
	
	# If EOF has been reached, readline() should return empty string and exit while loop
	while line != '':
		line = f.readline()
		
		# If program has reached a "header" line like CID..., INSTRUCTOR(S)..., or SEAT...,
		# the program will use that line to determine what info is in the next lines.
		header = line.replace('"', '').split(',')
		
		
		if header[0] == 'CID':
			# Enter this information for a given course(?) in the database's "Course" table.
			line = f.readline()
			course_arr = line.replace('"', '').split(',')

			# Set the variables for course from the array;
			# We will need these for later queries

			Cid = course_arr[0]
			Term = course_arr[1]
			Subj = course_arr[2]
			Crse = course_arr[3]
			Sec = course_arr[4]


			# course_arr is an array containing strings of information for a course
			# Different elements of course_arr are inserted into the corresponding parts of the Course table
			# Use the '-' character to determine if there are variable units:
			min_max_units = course_arr[5].replace('.000', '').split(' - ')
				
			if '-' in course_arr[5]:
				cur.execute("INSERT INTO Course (cid, term, subj, crse, sec, min_units, max_units) VALUES (%s, %s, %s, %s, %s, %s, %s)", 
				(Cid, Term, Subj, Crse, Sec, min_max_units[0], min_max_units[1]))
				
			else:
				cur.execute("INSERT INTO Course (cid, term, subj, crse, sec, min_units, max_units) VALUES (%s, %s, %s, %s, %s, %s, %s)", 
				(Cid, Term, Subj, Crse, Sec, min_max_units[0], min_max_units[0]))
			C.commit()


		elif header[0] == 'INSTRUCTOR(S)':
			# Need a loop here, because there may be more than one instructor
			# Enter this information for a given course(?) in the database's "Instructor" table.
			batch_inst_list = []
			batch_cNDi_list = []

			line = f.readline()
			while ',' in line:
				inst_arr = line.split('",')

				# Set the variables for inst from the array;
				# We will need these for later queries

				Inst = inst_arr[0]
				Type = inst_arr[1]
				Days = inst_arr[2]
				Time = inst_arr[3]
				Build = inst_arr[4]
				Room = inst_arr[5]

				# Fully take out quotes
				Inst = Inst.replace('"', '')
				Type = Type.replace('"', '')
				Days = Days.replace('"', '')
				Time = Time.replace('"', '')
				Build = Build.replace('"', '')
				Room = Room.replace('"', '')
				Room = Room.strip()

				if '\'' in Inst:
					Inst = Inst.replace('\'', '\'\'') 
				
				batch_inst_list.append([Inst, Type, Days, Time, Build, Room])
				if Inst:
					batch_cNDi_list.append([Cid, Term, Inst, Type, Days, Time])
				
				line = f.readline()

			# Insert data into the Instructor table
			cur.executemany("INSERT INTO Instructor VALUES (%s, %s, %s, %s, %s, %s)", 
			batch_inst_list)
			C.commit()

			# Insert data into the courseNDinst table
			if batch_cNDi_list:
				cur.executemany("INSERT INTO courseNDinst VALUES (%s, %s, %s, %s, %s, %s)", 
				batch_cNDi_list)
				C.commit()


		elif header[0] == 'SEAT':
			# Enter this information for a given course(?) in the database's "Student" table.
			batch_stud_list = []
			batch_cNDs_list = []
			batch_sNDi_list = []

			line = f.readline()
			while ',' in line:
				stud_arr = line.replace('"', '').split(',')

				# Set the variables for stud from the array;
				# We will need these for later queries

				Seat = stud_arr[0]
				Sid = stud_arr[1]
				Sur = stud_arr[2]
				Pref = stud_arr[3]
				Level = stud_arr[4]
				Units = stud_arr[5]
				Class = stud_arr[6]
				Major = stud_arr[7]
				Grade = stud_arr[8]
				Status = stud_arr[9]
				Email = stud_arr[10]
				Email = Email.strip()

				if not Units:
					Units = '0';
				
				Units = str(Units);					

				batch_stud_list.append([Sid, Sur, Pref, Level, Units, Class, Major, Grade, Status, Email])
				batch_cNDs_list.append([Cid, Term, Sid, Seat, Units, Grade])
				
				for instructor in batch_inst_list:
					if instructor[0]:
						batch_sNDi_list.append([Cid, Term, Sid, instructor[0], instructor[2], instructor[3], Grade])

				line = f.readline()

			# Insert data into the Student table
			cur.executemany("INSERT INTO Student VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", 
			batch_stud_list)
			C.commit()

			# Insert data into the courseNDstud table
			cur.executemany("INSERT INTO courseNDstud VALUES (%s, %s, %s, %s, %s, %s)", 
			batch_cNDs_list)
			C.commit()

			# Insert data into the studNDinst table
			if batch_sNDi_list:
				cur.executemany("INSERT INTO studNDinst VALUES (%s, %s, %s, %s, %s, %s, %s)", 
				batch_sNDi_list)
				C.commit()

					
	# Quick query test to see everything that has been written to the database
	# If you do fetchall instead of fetchone this should result in a looooooong list, so don't be scared
	
	cur.execute("SELECT * FROM Course;")
	cids_test = cur.fetchone()
	print cids_test

	cur.execute("SELECT * FROM Instructor;")
	inst_test = cur.fetchone()
	print inst_test

	cur.execute("SELECT * FROM Student;")
	sid_test = cur.fetchone()
	print sid_test

	cur.execute("SELECT * FROM courseNDinst;")
	cNDi_test = cur.fetchone()
	print cNDi_test

	cur.execute("SELECT * FROM courseNDstud;")
	cNDs_test = cur.fetchone()
	print cNDs_test
	
	cur.execute("SELECT * FROM studNDinst;")
	sNDi_test = cur.fetchone()
	print sNDi_test
	
	
	return


# This attempts to connect to the server. Should work
try:
	C = psycopg2.connect("dbname='postgres'")
except:
	print "I am unable to connect to the database"


# Establishes cursor to use with postgres database
cur = C.cursor()

# I need a string to use for the name in the main for loop
filename = ''

for file in os.listdir('Grades'):
	filename = 'Grades/' + file
	main(filename)
