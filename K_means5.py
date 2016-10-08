#!/usr/bin/python
import sys
from math import fabs
from org.apache.pig.scripting import Pig


MAX_ITERATION = 100
iter_num = 1
Pig.fs("rmr Centroid_file")
Pig.fs("rmr Centroid_List")
Pig.fs("rmr Station_List")


k=6
flag = 1;

CLuster_Assignment = Pig.compile(""" 
					REGISTER datafu-0.0.4.jar
					DEFINE haversineMiles datafu.pig.geo.HaversineDistInMiles();
					sep = LOAD '$filename' USING PigStorage (',');
					stations = DISTINCT (FOREACH sep GENERATE (int)$3, (double)$5 , (double)$6);

					temp = LIMIT sep 1;
					temp2 = FOREACH temp GENERATE '$newcen';
					temp3 = FOREACH temp2 GENERATE FLATTEN(TOKENIZE($0,',')) AS string;
					temp4 = FOREACH temp3 GENERATE FLATTEN(STRSPLIT(string,':',2)) AS (s:chararray,t:chararray);
					temp5= foreach temp4 generate $0 as id:chararray, FLATTEN(TOBAG(*)) as value:chararray;
					temp6 = FILTER temp5 BY id != value ;
					centroids = FOREACH temp6 GENERATE (double)$0 , (double)$1;
		
					C = CROSS centroids, stations;
					Dist = FOREACH C GENERATE $2,$0,$1, haversineMiles($0,$1,$3,$4);
					g = GROUP Dist BY $0;
					g2 = FOREACH g GENERATE $0, MIN(Dist.$3);
					g3 = JOIN g2 BY ($0,$1) , Dist BY ($0,$3);
					Clustered_data = FOREACH g3 GENERATE $0, $3, $4;

					STORE Clustered_data INTO 'Centroid_file' USING PigStorage (',');
					STORE stations INTO 'Station_List' USING PigStorage (',');
					""")
Next_Centroid = Pig.compile(""" 
					data = LOAD 'Centroid_file' USING PigStorage (',');
					stations = LOAD 'Station_List' USING PigStorage (',');
					temp = LIMIT stations 1;
					splited_data = FOREACH temp GENERATE FLATTEN(STRSPLIT('$string',':',2)) AS (s:chararray,t:chararray);
					filtered_data = FILTER data BY ($1 == splited_data.$0) AND ($2 == splited_data.$1);

					Clustered_data = FOREACH filtered_data GENERATE (int)$0;
					C_temp1 = JOIN Clustered_data BY $0, stations BY $0;
					C_temp2 = GROUP C_temp1 ALL;
					boom = FOREACH C_temp2 GENERATE AVG(C_temp1.$2) , AVG(C_temp1.$3);
					boom2 = FOREACH boom GENERATE ROUND($0*10000000.0)/10000000.0 , ROUND($1*10000000.0)/10000000.0;
					newCen= FOREACH boom2 GENERATE CONCAT(CONCAT((chararray)$0,':'),(chararray)$1);
					
					STORE newCen INTO 'Centroid_List' USING PigStorage (',');
					""")


#current_centroids = "40.7475061:-73.9868993,40.7197565:-74.0029409,40.6926741:-73.9711446,40.7622742:-73.9847168,40.7264692:-73.9855679,40.7015954:-74.0015412"
current_centroids = "40.7445758:-73.9921911,40.7202095:-74.0030401,40.6926741:-73.9711446,40.7595904:-73.9801875,40.7254868:-73.9841834,40.702131:-74.0011912"
Q2 = CLuster_Assignment.bind({'filename':"sep_revised.csv",'newcen':current_centroids})
results2 = Q2.runSingle()
"""
centroids_array = [None] * k
centroids_array[0] = "40.7475061:-73.9868993"
centroids_array[1] = "40.7197565:-74.0029409"
centroids_array[2] = "40.6926741:-73.9711446"
centroids_array[3] = "40.7622742:-73.9847168"
centroids_array[4] = "40.7264692:-73.9855679"
centroids_array[5] = "40.7015954:-74.0015412"

iter_num = 1
while flag == 1 and iter_num<6:
	Pig.fs("rmr Centroid_file")
	Pig.fs("rmr Station_List")
	iter_num += 1
	flag = 0
	Q2 = CLuster_Assignment.bind({'filename':"sep_revised.csv",'newcen':current_centroids})
	results2 = Q2.runSingle()
	print iter_num
	current_centroids = ""
	for i in range(k):
		Pig.fs("rmr Centroid_List")
		Q3 = Next_Centroid.bind({'string':centroids_array[i]})
		results3 = Q3.runSingle()
		iter = results3.result("newCen").iterator()
		tuple = str(iter.next().get(0))
		current_centroids = current_centroids + tuple + ","
		if tuple != centroids_array[i]:
			flag = 1	
		centroids_array[i] = tuple	
		print current_centroids
		print centroids_array[i]
		print iter_num
	current_centroids = current_centroids[0:len(current_centroids)-1]
	print flag

	"""