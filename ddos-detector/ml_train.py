from joblib import dump, load
import pandas as pd 
import numpy as np
from datetime import datetime
import pickle

from sklearn.model_selection import train_test_split
from sklearn.base import ClassifierMixin
from sklearn.metrics import roc_auc_score, confusion_matrix

from sklearn.neighbors import KNeighborsClassifier

from es_connection import ESConnection
from typing import List, Dict
import ml_utils

def train_model(model: ClassifierMixin, data_time_range: List[str], output_path: str):
    es_host = ESConnection(es_host='http://localhost:9200', logstash_host='http://localhost:5000')

    dataset = ml_utils.get_data(start_time=data_time_range[0], end_time=data_time_range[1], es_host=es_host)
    dataset.to_pickle('data/dataset.pkl')
    dataset = pd.read_pickle('data/dataset.pkl')
    print(len(dataset.columns))

    y = dataset['target']
    X = dataset.drop(columns=['target'])

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.33, random_state=17)
    print('Training model')
    model = model.fit(X_train, y_train)
    print('Finished training')
    prediction = model.predict(X_test)
    print(confusion_matrix(y_test, prediction))

    dump(model, output_path + '/' + type(model).__name__ + '.joblib')

if __name__ == "__main__":
    model_list = [
        KNeighborsClassifier(n_neighbors=5)
    ]

    for model in model_list:
        train_model(model=model, data_time_range=['2020-06-20T12:00:00.000Z', '2020-07-04T01:10:00.000Z'], output_path='saved_models')
