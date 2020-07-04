import pandas as pd
from es_connection import ESConnection
from typing import List, Dict

def get_df_dummies(dataset: pd.DataFrame, index_col: str, col_list: List[str]):
    dataset = dataset.set_index(index_col)
    for column in col_list:
        dataset = pd.concat([dataset, pd.get_dummies(dataset[column])]) 
    dataset = dataset.drop(columns=col_list)
    dataset = dataset.fillna(0)
    return dataset

def calculate_features(dataset: pd.DataFrame) -> pd.DataFrame:
    unique_ips = dataset['src_ip'].value_counts().keys()

    result_list = []
    for ip in unique_ips:
        tmp = dataset[dataset['src_ip'] == ip]
        if tmp['raw'].value_counts().keys().tolist()[0] == "b'0'":
            target = 0
        else:
            target = 1
        result_dict = {
            'src_ip': ip,
            'count': tmp.shape[0],
            'src_mac_count': len(tmp['src_mac'].value_counts()),
            'dst_mac_count': len(tmp['dst_mac'].value_counts()),
            'src_port_count': len(tmp['src_port'].value_counts()),
            'dst_port_count': len(tmp['dst_port'].value_counts()),
            'dst_ip_count': len(tmp['dst_ip'].value_counts()),
            'most_freq_dst_port': tmp['dst_port'].value_counts().keys().tolist()[0],
            'most_freq_dst_mac': tmp['dst_mac'].value_counts().keys().tolist()[0],
            'most_freq_dst_ip': tmp['dst_ip'].value_counts().keys().tolist()[0],
            'target': target
        }
        result_list.append(result_dict)

    dataset = pd.DataFrame(result_list)
    dataset = get_df_dummies(dataset=dataset, index_col='src_ip', col_list=['most_freq_dst_port', 'most_freq_dst_mac', 'most_freq_dst_ip'])
    return dataset

def calculate_ip_features(dataset: pd.DataFrame, ip: str) -> pd.DataFrame:
    result_list = []
    tmp = dataset[dataset['src_ip'] == ip]
    if tmp['raw'].value_counts().keys().tolist()[0] == "b'0'":
        target = 0
    else:
        target = 1
    result_dict = {
        'src_ip': ip,
        'count': tmp.shape[0],
        'src_mac_count': len(tmp['src_mac'].value_counts()),
        'dst_mac_count': len(tmp['dst_mac'].value_counts()),
        'src_port_count': len(tmp['src_port'].value_counts()),
        'dst_port_count': len(tmp['dst_port'].value_counts()),
        'dst_ip_count': len(tmp['dst_ip'].value_counts()),
        'most_freq_dst_port': tmp['dst_port'].value_counts().keys().tolist()[0],
        'most_freq_dst_mac': tmp['dst_mac'].value_counts().keys().tolist()[0],
        'most_freq_dst_ip': tmp['dst_ip'].value_counts().keys().tolist()[0],
        'target': target
    }
    result_list.append(result_dict)

def get_data(start_time: str, end_time: str, es_host: ESConnection) -> pd.DataFrame:
    fields = ['raw', 'src_ip', 'src_mac', 'src_port', 'dst_ip', 'dst_mac', 'dst_port', '@timestamp', '_id']
    data_df = es_host.index_data_to_df(index_name='input_ryu', start_time=start_time, end_time=end_time, field_list=fields)
    print('Calculating features')
    data_df = calculate_features(data_df)
    print('Finished calculating features')
    return data_df

def get_ip_data(es_host: ESConnection) -> pd.DataFrame:
    fields = ['raw', 'src_ip', 'src_mac', 'src_port', 'dst_ip', 'dst_mac', 'dst_port', '@timestamp', '_id']
    data_df = es_host.index_data_to_df(index_name='input_ryu', start_time=start_time, end_time=end_time, field_list=fields)

