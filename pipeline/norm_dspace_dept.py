#!/usr/bin/env python

import sys
import json
import requests
import fileinput

def main():
	for line in fileinput.input():
		data = json.loads(line)
		
		# TODO: 

		# Send department name to lookup service
		# r = requests.get('http://hostname/norm_dspace_dept?dept='+data['department'])

		# if no match
		#	 keep dept name as is.  no variant exists.
		# else
		#    data['department'] = r
		
		data['department'] = 'testing_changed_dept'

		# Emit JSON with updated department
		print(json.dumps(data))

if __name__ == '__main__':
	
	if "-h" in sys.argv:
		help = """
	To use, list handle(s) as arguments or read from stdin

	Example usage:
		./norm_dspace_dept.py dept1 dept2 dept3
		./norm_dspace_dept.py < dept1
		echo dept1 | ./norm_dspace_dept.py"""

		print(help)
		sys.exit(0)

	main()