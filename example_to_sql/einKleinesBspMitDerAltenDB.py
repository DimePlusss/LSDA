# -*- coding: utf-8 -*-
"""
Created on Mon Nov  6 12:03:44 2023

@author: Dime
"""

import warnings
import os
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from example_pipelines.healthcare.healthcare_utils import MyW2VTransformer, \
     create_model
from mlinspect.utils import get_project_root
from example_pipelines.healthcare.healthcare_utils import MyW2VTransformer, create_model


COUNTIES_OF_INTEREST = ['county2', 'county3']

patients = pd.read_csv(
    os.path.join( "C:\\Users\Dime\SQLundMlinspect\mlinspect\example_pipelines\healthcare\patients.csv"),
    na_values='?')
histories = pd.read_csv(
    os.path.join( "C:\\Users\Dime\SQLundMlinspect\mlinspect\example_pipelines\healthcare\histories.csv"),
    na_values='?')

data = patients.merge(histories, on=['ssn'])
complications = data.groupby('age_group').agg(mean_complications=('complications', 'mean'))
data = data.merge(complications, on=['age_group'])
data['label'] = data['complications'] > 1.2 * data['mean_complications']
data = data[['smoker', 'last_name', 'county', 'num_children', 'race', 'income', 'label']]
data = data[data['county'].isin(COUNTIES_OF_INTEREST)]
impute_and_one_hot_encode = Pipeline([
    ('impute', SimpleImputer(strategy='most_frequent')),
    ('encode', OneHotEncoder(sparse=False, handle_unknown='ignore'))
])
featurisation = ColumnTransformer(transformers=[
    ("impute_and_one_hot_encode", impute_and_one_hot_encode, ['smoker', 'county', 'race']),
    # ('word2vec', MyW2VTransformer(min_count=2), ['last_name']),
    ('numeric', StandardScaler(), ['num_children', 'income']),
], remainder='drop')
#neural_net = MyKerasClassifier(build_fn=create_model, epochs=10, batch_size=1, verbose=0)
pipeline = Pipeline([
    ('features', featurisation)#,
#    ('learner', neural_net)
])
train_data, test_data = train_test_split(data)
model = pipeline.fit(train_data, train_data['label'])
#print("Mean accuracy: {}".format(model.score(test_data, test_data['label'])))