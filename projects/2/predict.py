#!/opt/conda/envs/dsenv/bin/python

import sys, os
import logging
from joblib import load
import pandas as pd

sys.path.append('.')

#from model import numeric_features
numeric_features = ["if"+str(i) for i in range(1,14)]

#
# Init the logger
#
logging.basicConfig(level=logging.DEBUG)
logging.info("CURRENT_DIR {}".format(os.getcwd()))
logging.info("SCRIPT CALLED AS {}".format(sys.argv[0]))
logging.info("ARGS {}".format(sys.argv[1:]))

#load the model
model = load("2.joblib")

#read and infere
read_opts=dict(
        sep='\t', names=["id"] + numeric_features, index_col=False, header=None,
        iterator=True, chunksize=100
)

logging.info("Predict data ...")
logging.info(numeric_features)

for df in pd.read_csv(sys.stdin, **read_opts):
    #df['id'] = df['id'].astype(int)
    #for col in numeric_features:
    #    df[col] = df[col].astype(float)
    pred = model.predict_proba(df.iloc[:, 1:])
    out = zip(df.id, pred[:, 1])
    print("\n".join(["{0}\t{1}".format(*i) for i in out]))
logging.info("... completed")
