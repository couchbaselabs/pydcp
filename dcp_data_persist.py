""" Functions for editing JSON format files which deal with log updates,
    to persist uuids and seq_nos for each vbucket.
    Currently stored within folder called 'logs'
    Potential scope to save more data about vbucket if needed
"""

import json
import os


def get_path(vb):
    """ Retrieves path to log file for inputted virtual bucket number """
    dir_path = os.path.dirname(os.path.realpath(__file__))
    json_ext = os.path.normpath('logs/' + str(vb) + '.json')
    path = os.path.join(dir_path, json_ext)

    # Make directory if it doesn't exist
    dirname = os.path.dirname(path)
    if dirname and not os.path.exists(dirname):
        os.mkdir(dirname)

    return path


def reset(vb_list):
    """ Clears/makes files for list of virtual bucket numbers"""
    for vb in vb_list:
        path_string = get_path(vb)
        with open(path_string, 'w') as f:
            json.dump({}, f)


def upsert_failover(vb, failover_log):
    """ Insert / update failover log """
    path_string = get_path(vb)

    with open(path_string, 'r') as vb_log:
        data = json.load(vb_log)

    data['failover_log'] = failover_log

    with open(path_string, 'w') as vb_log:
        json.dump(data, vb_log)


def upsert_sequence_no(vb, seq_no):
    """ Insert / update sequence number, and move old sequence number to appropriate list """
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
    """ Return a dictionary where keys are vbuckets and the data is the total JSON for that vbucket """
    read_dict = {}

    for vb in vb_list:
        path_string = get_path(vb)
        with open(path_string, 'r') as vb_log:
            data = json.load(vb_log)
        read_dict[str(vb)] = data

    return read_dict


def get_seq_nos(vb_list):
    """ Return a dictionary where keys are vbuckets and the data is the sequence number """
    read_dict = {}

    for vb in vb_list:
        path_string = get_path(vb)
        with open(path_string, 'r') as vb_log:
            data = json.load(vb_log)
        read_dict[str(vb)] = data['seq_no']

    return read_dict


def get_failover_logs(vb_list):
    """ Return a dictionary where keys are vbuckets and the data is the failover log list """
    read_dict = {}

    for vb in vb_list:
        path_string = get_path(vb)
        with open(path_string, 'r') as vb_log:
            data = json.load(vb_log)
        read_dict[str(vb)] = data['failover_log']

    return read_dict
