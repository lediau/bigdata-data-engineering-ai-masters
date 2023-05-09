create temporary external table hw2_test 
(id int, if1 double, if2 double, if3 double, if4 double, if5 double, if6 double, if7 double, if8 double, if9 double, if10 double, 
 if11 double, if12 double, if13 double)
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with serdeproperties('separatorChar'='\t', 'quoteChar'='\"')
stored as textfile
location "/datasets/criteo/criteo_test_large_features";
