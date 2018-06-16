#!/bin/python

import pprint # only for debugging
from pymongo import MongoClient
import random
import datetime
import time

import sys




""" AutoSparc 

Usage: python connect_inject_run.py [exp] [time]

[exp] -> Experiment number from cryosparc that serves as a template (needs to be created in cryosparc before)
[time] -> in minutes, the intervall of new job creation


The script first reads the template experiment from the CryoSPARC database, changes a few fields (experiment number, name, date etc.) and re-inserts a new experiment with the same settings as the template. Then A new job for that experiment is created in the data base and is marked as "queued". Then Cryosparc will start the queued job autmatically. 

Please note: This script does not have proper exception handling. In case of errors, funny things can happen in cryosparc ;-)

"""




""" This is used to create the ids for the mongo DB """
def random_string_id(num_chars=17):
	UNMISTAKABLE_CHARS = "23456789ABCDEFGHJKLMNPQRSTWXYZabcdefghijkmnopqrstuvwxyz"
	return "".join([random.choice(UNMISTAKABLE_CHARS) for i in range(num_chars)])


def get_next_id(exp_col, key):
# Input is a collection pointer and the key that is used for the ID (ID is int)
	ids = []
	for doc in exp_col.find({}):
		# some jobs dont have a queue index??? who cares
		if key in doc.keys():
			ids.append(int( doc[key] ))
	return max(ids)+1





print "Starting AutoSparc"
print "Using experiment #"+str(sys.argv[1])+" as template"
print "Restarting a new Experiment every"+str(int(sys.argv[2])*60)+" minutes"


while 1:
	# Change server to the adress you need if the scriot does not run on the same machine
	# The standard port is 38001, however, it may have been changed during your installation
	client = MongoClient("localhost",38001)
	# make a DB pointer
	db = client.meteor

	# make pointers for collections(tables)
	sparc_experiments = db["experiments"]
	sparc_jobs = db["jobs"]

	CLONE_EXP = int(sys.argv[1])
	#-> this will also contain the dataset, user, experimental set up etc
	# -> make sure the template experiment is not deleted. Maybe load it once and then keep it as a template?

	# Extract the template experiment as a dictonary
	orig = sparc_experiments.find_one( {"uid": CLONE_EXP} )
	#print type(orig)

	# Vals to be updated:
	# _id -> with random number
	# createdAt


	# Job ID is only updated once there was a job stated. -> check order in sparcjob.py
	# job_id -> with random number -> keep this info for the job entry!

	#print orig["_id"]
	#print orig["createdAt"]

	new_id = random_string_id()
	new_job_id = random_string_id()
	orig["_id"] = new_id
	orig["createdAt"] = datetime.datetime.utcnow()
	orig["uid"] = get_next_id(sparc_experiments, "uid")
	orig["job_id"] = new_job_id
	orig["exp_name"] = time.strftime("%d/%m/%Y - %H:%M")

	#print orig["_id"]
	#print orig["createdAt"]
	#print orig["uid"]

	res = sparc_experiments.insert_one(orig)

	#print res


	# raw template for constructing a new job
	"""
	{   u'_id': u'7J4L6Sqw98ZfJ2evJ',
	    u'dataset_id': u'8MCWw638uhhW2rT4P',
	    u'experiment_id': u'bov9G6s2ZPt3FDuqT',
	    u'experiment_uid': 341,
	    u'job_log_dir_rel': u'7J4L6Sqw98ZfJ2evJ',
	    u'job_type': u'class2D',
	    u'queue_index': 320,
	    u'queuedAt': datetime.datetime(2018, 2, 25, 11, 22, 26, 815000),
	    u'status': u'queued',
	    u'user_id': u'7q7MKkj8KvWk8EEYB',
	    u'user_is_admin': False,
	    u'user_is_super_admin': False,
	    u'user_name': {   u'first': u'Dim', u'last': u'Teg'}}
	"""
	# constructs the new job
	new_job = {
		"_id" : new_job_id,
		"dataset_id" : orig["dataset_id"],
		"experiment_id" : orig["_id"],
		"experiment_uid" : orig["uid"],
		"job_log_dir_rel" : new_job_id,
		"job_type" : orig["task"],
		"queue_index" : get_next_id(sparc_jobs, "queue_index"),
		"queuedAt" : datetime.datetime.utcnow(),
		"status" : "queued",
		"user_id" : orig["createdBy"]["userId"],
		"user_is_admin" : False,
		"user_is_super_admin" : False,
		"user_name" : { "first" : orig["createdBy"]["userName"].split(" ")[0], "last" : orig["createdBy"]["userName"].split(" ")[1] }
	}

	#print new_job
	# inserts the new job
	res = sparc_jobs.insert_one(new_job)
	
	print str(datetime.datetime.utcnow())+": Started new experiment"

	#print res
	time.sleep(int(sys.argv[2])*60)
