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

def calculate_ip_features(dataset: pd.DataFrame, ip: str) -> List:
    result_list = []
    if dataset['raw'].value_counts().keys().tolist()[0] == "b'0'":
        target = 0
    else:
        target = 1
    result_dict = {
        'src_ip': ip,
        'count': dataset.shape[0],
        'src_mac_count': len(dataset['src_mac'].value_counts()),
        'dst_mac_count': len(dataset['dst_mac'].value_counts()),
        'src_port_count': len(dataset['src_port'].value_counts()),
        'dst_port_count': len(dataset['dst_port'].value_counts()),
        'dst_ip_count': len(dataset['dst_ip'].value_counts()),
        'most_freq_dst_port': dataset['dst_port'].value_counts().keys().tolist()[0],
        'most_freq_dst_mac': dataset['dst_mac'].value_counts().keys().tolist()[0],
        'most_freq_dst_ip': dataset['dst_ip'].value_counts().keys().tolist()[0],
        'target': target
    }
    result_list.append(result_dict)
    return result_list

def calculate_features(dataset: pd.DataFrame) -> pd.DataFrame:
    unique_ips = dataset['src_ip'].value_counts().keys()

    result_list = []
    for ip in unique_ips:
        dataset = dataset[dataset['src_ip'] == ip]
        result_list.extend(calculate_ip_features(dataset=dataset, ip=ip))

    dataset = pd.DataFrame(result_list)
    dataset = get_df_dummies(dataset=dataset, index_col='src_ip', col_list=['most_freq_dst_port', 'most_freq_dst_mac', 'most_freq_dst_ip'])
    return dataset

def get_data(start_time: str, end_time: str, es_host: ESConnection) -> pd.DataFrame:
    fields = ['raw', 'src_ip', 'src_mac', 'src_port', 'dst_ip', 'dst_mac', 'dst_port', '@timestamp', '_id']
    data_df = es_host.index_data_to_df(index_name='input_ryu', start_time=start_time, end_time=end_time, field_list=fields)
    print('Calculating features')
    data_df = calculate_features(data_df)
    print('Finished calculating features')
    return data_df

def get_ip_data(ip: str, start_time: str, end_time: str, es_host: ESConnection) -> pd.DataFrame:
    fields = ['raw', 'src_ip', 'src_mac', 'src_port', 'dst_ip', 'dst_mac', 'dst_port', '@timestamp', '_id']
    data_df = es_host.ip_data_to_df(index_name='input_ryu', start_time=start_time, end_time=end_time, field_list=fields, ip=ip)
    print('Calculating ip features for: {}'.format(ip))
    data_df = calculate_ip_features(dataset=data_df, ip=ip)
    print('Finished calculating features for: {}'.format(ip))
    return data_df

