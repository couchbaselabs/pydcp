# Functions for dealing with log updates, to persist uuids and seq_nos for each vbucket

import json
import os


def get_path(vb):
    dir_path = os.path.dirname(os.path.realpath(__file__))
    path = str(dir_path) + '/logs/' + str(vb) + '.json'
    if not os.path.exists(path):  # The file doesn't exist
        # Raise exception if the directory that should contain the file doesn't exist
        dirname = os.path.dirname(path)
        if dirname and not os.path.exists(dirname):
            raise IOError(
                ("Could not initialize empty JSON file in non-existant "
                 "directory '{}'").format(os.path.dirname(path))
            )

    return path


def reset(vb_list):
    for vb in vb_list:
        path_string = get_path(vb)
        with open(path_string, 'w') as f:
            json.dump({}, f)


def upsert_failover(vb, failover_log):
    path_string = get_path(vb)

    with open(path_string, 'r') as vb_log:
        data = json.load(vb_log)

    data['failover_log'] = failover_log

    with open(path_string, 'w') as vb_log:
        json.dump(data, vb_log)


def upsert_sequence_no(vb, seq_no):
    path_string = get_path(vb)

    with open(path_string, 'r') as vb_log:
        data = json.load(vb_log)

    if 'old_seq_no' in data.keys():
        old_seq_no = data['old_seq_no']
    else:
        old_seq_no = []

    if 'seq_no' in data.keys():
        old_seq_no.append(data['seq_no'])
    data['old_seq_no'] = old_seq_no
    data['seq_no'] = seq_no

    with open(path_string, 'w') as vb_log:
        json.dump(data, vb_log)


def read_all(vb_list):
    read_dict = {}

    for vb in vb_list:
        path_string = get_path(vb)
        with open(path_string, 'r') as vb_log:
            data = json.load(vb_log)
        read_dict[str(vb)] = data

    return read_dict


def get_seq_nos(vb_list):
    read_dict = {}

    for vb in vb_list:
        path_string = get_path(vb)
        with open(path_string, 'r') as vb_log:
            data = json.load(vb_log)
        read_dict[str(vb)] = data['seq_no']

    return read_dict


def get_failover_logs(vb_list):
    read_dict = {}

    for vb in vb_list:
        path_string = get_path(vb)
        with open(path_string, 'r') as vb_log:
            data = json.load(vb_log)
        read_dict[str(vb)] = data['failover_log']

    return read_dict
