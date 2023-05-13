create table hw2_pred
(id int, pred double)
row format delimited
fields terminated by "\t"
stored as textfile
location "lediau_hw2_pred";