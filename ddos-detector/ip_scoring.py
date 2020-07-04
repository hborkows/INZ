import pandas as pd 
import pickle
from joblib import load
from sklearn.base import ClassifierMixin
from es_connection import ESConnection
import ml_utils

class Classifier:
    def __init__(self, saved_model_path: str, es_host: str):
        self._model: ClassifierMixin = load(saved_model_path)
        self._es_con = ESConnection(es_host=es_host)

    def score_ip(self, ip: str):
        ip_df: pd.DataFrame = ml_utils.get_ip_data(ip=ip, es_host=self._es_con)
        return self._model.predict_proba(ip_df)
