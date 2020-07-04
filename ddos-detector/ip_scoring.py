import pandas as pd 
import pickle
from joblib import load
from sklearn.base import ClassifierMixin
import ml_utils

class Classifier:
    def __init__(self, saved_model_path: str):
        self.model: ClassifierMixin = load(saved_model_path)

    def score_ip(self, ip: str):
        ip_df: pd.DataFrame = ml_utils
