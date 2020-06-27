"""etcd3 Leader election."""
import etcd3
import sys
import time
from threading import Event

# REFERENCE:  https://www.sandtable.com/etcd3-leader-election-using-python

LEADER_KEY = '/beingzero/storage/leader'
LEASE_TTL = 5
SLEEP = 1
 
def main(server_name):
    client = etcd3.client(host="127.0.0.1", port=2379)
    while True:
        is_leader, lease = leader_election(client, server_name)

        if is_leader:
            print('I am Leader')
            leader_proceeds(lease)
        else:
            print('I am Follower/Standby')
            follower_proceeds(client)


def follower_proceeds(client):
    election_event = Event()
    def watch_callback(resp):
        for event in resp.events:
            if isinstance(event, etcd3.events.DeleteEvent):
                print("Leadership need to change")
                election_event.set()

    watch_id = client.add_watch_callback(LEADER_KEY, watch_callback)

    try:
        while not election_event.is_set():
            time.sleep(SLEEP)
    except (KeyboardInterrupt):
        client.cancel_watch(watch_id)
        sys.exit(1)

    client.cancel_watch(watch_id)


# if key is already present, it'll throw exception
def put_not_exist(client, key, value, lease=None):
    status, _ = client.transaction(
        failure=[],
        success=[client.transactions.put(key, value, lease)],
        compare=[client.transactions.version(key) == 0]
    )
    return status


def leader_election(client, server_name):
    print('Leader election - started')
    try:
        lease = client.lease(LEASE_TTL)
        is_leader = put_not_exist(client, LEADER_KEY, server_name, lease)
    except Exception:
        is_leader = False
    return is_leader, lease


def leader_proceeds(lease):
    try:
        while True:
            # do work
            print('I am a Leader - trying to Refresh the lease')
            lease.refresh()
            do_work()
    except Exception:
        lease.revoke()
        return
    except KeyboardInterrupt:
        print('Revoking Lease')
        lease.revoke()
        sys.exit(1)


def do_work():
    time.sleep(SLEEP)
   
if __name__ == '__main__':
    server_name = sys.argv[1]
    main(server_name)