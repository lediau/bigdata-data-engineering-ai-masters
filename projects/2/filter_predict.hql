add file projects/2/predict.py;
add file 2.joblib;

insert into table hw2_pred
select transform(*) using "/opt/conda/envs/dsenv/bin/python predict.py" as (id int, pred double)
from hw2_test
where cast(if1 as double) > 20 and cast(if1 as double) < 40