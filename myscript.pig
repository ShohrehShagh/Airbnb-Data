sep = LOAD 'sep.csv' USING PigStorage(',');
/* AS (trip_duration:int, start_time: chararray, stop_time:chararray, start_station_ID:int, start_station_name:chararray, start_station_latitude:double, start_station_longitude:double, end_station_ID:int, end_station_name: chararray, end_station_latitude:double, end_station_longitude:double, bikeid: int, user_type:chararray, birth_year:chararray, gender:int);*/

unfiltered_attributes_sep = FOREACH sep GENERATE $3 AS start_station, $7 AS end_station;
attributes_sep = FILTER unfiltered_attributes_sep BY start_station!=end_station;
grouped_pairs_sep = group attributes_sep by (start_station,end_station);
counted_pairs_sep= FOREACH grouped_pairs_sep GENERATE group as grp, COUNT(attributes_sep.start_station)as num_pairs_sep;

grouped_stations_sep = group attributes_sep by (start_station);
counted_stations_sep = FOREACH grouped_stations_sep GENERATE group as grp2, COUNT(attributes_sep.start_station)as num_stations_sep;
ordered_counted_stations_sep = ORDER counted_stations_sep BY num_stations_sep DESC;
top_source_stations_sep = LIMIT ordered_counted_stations_sep 6;

stations = DISTINCT (FOREACH sep GENERATE $3);
top_stations_trips = CROSS (FOREACH top_source_stations_sep GENERATE $0),stations;
grouped_top_stations_trips = group top_stations_trips by ($0,$1);

X = join grouped_top_stations_trips BY $0,counted_pairs_sep BY $0; 
X1 = FOREACH X GENERATE FLATTEN($0), $3 AS cluster_number;

STORE X1 INTO 'COUNTED_TRIPS' USING PigStorage (',');

