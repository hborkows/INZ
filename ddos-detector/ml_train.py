from joblib import dump, load
import pandas as pd 
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.base import ClassifierMixin
from sklearn.metrics import roc_auc_score, confusion_matrix
from es_connection import ESConnection
from typing import List, Dict

def calculate_features(dataset: pd.DataFrame) -> pd.DataFrame:
    unique_ips = dataset['src_ip'].value_counts().keys()

    result_list = []
    for ip in unique_ips:
        tmp = dataset[dataset['src_ip'] == ip]
        result_dict = {
            'src_ip': ip,
            'count': tmp.shape[0],
            'src_mac_count': len(tmp['src_mac'].value_counts()),
            'dst_mac_count': len(tmp['dst_mac'].value_counts()),
            'src_port_count': len(tmp['src_port'].value_counts()),
            'dst_port_count': len(tmp['dst_port'].value_counts()),
            'dst_ip_count': len(tmp['dst_ip'].value_counts())  
        }
        result_list.append(result_dict)

    dataset = pd.DataFrame(result_list)

    return dataset

def get_training_data(start_time: str, end_time: str, es_host: ESConnection) -> pd.DataFrame:
    fields = ['raw', 'src_ip', 'src_mac', 'src_port', 'dst_ip', 'dst_mac', 'dst_port']
    data_df = es_host.index_data_to_df(index_name='input_ryu', start_time=start_time, end_time=end_time, field_list=fields)
    print('Calculating features')
    data_df = calculate_features(data_df)
    print('Finished calculating features')
    return data_df

def train_model(model: ClassifierMixin, data_time_range: List[str], output_path: str):
    es_host = ESConnection(es_host='http://localohst:9200', logstash_host='http://localhost:5000')

    dataset = get_training_data(start_time='2020-06-20T12:00:00.000Z', end_time='2020-06-20T17:45:00.000Z', es_host=es_host)

    y = dataset['target']
    X = dataset.drop(columns=['target'])

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.33, random_state=17)
    print('Training model')
    model = model.fit(X_train, y_train)
    print('Finished training')
    prediction = model.predict(X_test)
    print(confusion_matrix(y_test, prediction))

    dump(model, output_path)
