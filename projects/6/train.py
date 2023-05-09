#!/opt/conda/envs/dsenv/bin/python

import os, sys
import logging

import pandas as pd
from joblib import dump
from sklearn.model_selection import train_test_split

from sklearn.ensemble import GradientBoostingClassifier

path_in, path_out = sys.argv[1], sys.argv[2]

logging.basicConfig(level=logging.DEBUG)

df = pd.read_csv(path_in)

X_train, X_test, y_train, y_test = train_test_split(
     df.iloc[:, 1:], df.iloc[:, 0], test_size=0.25, random_state=42
)

model = GradientBoostingClassifier()
model.fit(X_train, y_train)
logging.info("model fitted!")

model_score = model.score(X_test, y_test)
logging.info(f"model score: {model_score:.3f}")

dump(model, path_out)
logging.info("dumped!")