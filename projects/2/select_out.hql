insert overwrite directory 
'evercrimson_hiveout' 
row format delimited
fields terminated by '\t'
select * from hw2_pred;