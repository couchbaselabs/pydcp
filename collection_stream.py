
import pprint
import time
import argparse

import lib.dcp_bin_client
import constants

def collection_stream(host, port, user, password, bucket, filterFile):
    dcp_client = lib.dcp_bin_client.DcpClient(host, port, do_auth=False)
    op = dcp_client.sasl_auth_plain(user, password)
    op = dcp_client.bucket_select(bucket)

    json = ''
    if filterFile:
        filterFile = open(filterFile, "r")
        json = filterFile.read()

    print "DCP Open filter: {}".format(json)

    op = dcp_client.open_producer("mystream", collections=True, json=json)
    op = dcp_client.general_control("enable_noop", "true")
    op = dcp_client.stream_req(0, 0, 0, 0xffffffffffffffff, 0)
    while op.has_response():
        response = op.next_response()
        #print response
        if response:
            if response['opcode'] == constants.CMD_MUTATION:
                clen = response['collection_len']
                if clen > 0:
                    print 'RX: A document in a collection {} ({})'.format(response['key'][:clen], response['key'])
            if response['opcode'] == 0x5f:
                if response['event'] == 0:
                    print "RX: CreateCollection collection {}".format(response['key'])
                elif response['event'] == 1:
                    print "RX: DeleteCollection collection {}".format(response['key'])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("-f", "--filter", help="DCP Filter", required=False)
    parser.add_argument("-u", "--user", help="User", required=True)
    parser.add_argument("-p", "--password", help="Password", required=True)
    parser.add_argument("-n", "--node", help="Node host:port", required=True)
    parser.add_argument("-b", "--bucket", help="Bucket name", required=True)
    args = parser.parse_args()
    host, port = args.node.split(":")
    collection_stream(host, int(port), args.user, args.password, args.bucket, args.filter)
