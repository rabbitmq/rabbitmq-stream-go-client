__author__ = 'gabriele'

import base64
import time
import datetime
import json

import urllib.request
### This script removes all the queues, so be careful!!!

def print_time(step):
    ts = time.time();
    st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S');
    print(st + " - " + step)


def get_auth(username, password):
    credentials = ('%s:%s' % (username, password))
    encoded_credentials = base64.b64encode(credentials.encode('ascii'))
    return 'Authorization', 'Basic %s' % encoded_credentials.decode("ascii")


def call_api(rabbitmq_host, vhost, user, password):

    p = urllib.request.HTTPPasswordMgrWithDefaultRealm()
    p.add_password(None, "http://" + rabbitmq_host + ":15672/api/queues", user, password)

    auth_handler = urllib.request.HTTPBasicAuthHandler(p)
    opener = urllib.request.build_opener(auth_handler)

    urllib.request.install_opener(opener)

    req = urllib.request.Request("http://" + rabbitmq_host + ":15672/api/queues",
                                 method='GET')

    res = urllib.request.urlopen(req, timeout=5)

    print_time(" *** response done, loading json")
    queues = json.load(res)
    for q in queues:
        print_time(" *** removing " + q['name'])

        request_del = urllib.request.Request(
            "http://" + rabbitmq_host + ":15672/api/queues/" + vhost + "/" + q[
                'name'], method='DELETE')
        urllib.request.urlopen(request_del, timeout=5)
        print_time(" *** removed " + q['name'])


if __name__ == '__main__':
    rabbitmq_host = "localhost"
    call_api(rabbitmq_host, "%2f", "guest", "guest")
