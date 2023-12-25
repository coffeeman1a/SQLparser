import requests
import subprocess
import tempfile
import os

class Zabbix:
    def __init__(self, host, server, user, passwd, localhost=False):
        self.zabbix_url = "http://{}/api_jsonrpc.php".format(host) if not localhost else "http://localhost/zabbix/api_jsonrpc.php"
        self.host = host
        self.user = user
        self.passwd = passwd
        self.server = server
        self.zabbix_sender = '/usr/bin/zabbix_sender'
        self.headers = {
            'Content-Type': 'application/json-rpc',
            'User-Agent':   'python/requests',
            'Accept':       'application/json',
        }

        self.auth_payload = {
            "jsonrpc": "2.0",
            "method": "user.login",
            "params": {
                "user": self.user,
                "password": self.passwd
            },
            "id": 1,
        }

        self.auth_token = self.get_auth_token(self.zabbix_url, self.headers, self.auth_payload)

    def get_auth_token(self, zabbix_url, headers, auth_payload):
        auth_resp = requests.post(zabbix_url, headers=headers, json=auth_payload)
        auth_token = auth_resp.json()['result']
        return auth_token

    def send_to_zabbix(self, item_key, timestamp, value):
        with tempfile.NamedTemporaryFile(delete=False, mode='w') as tmpfile:
            tmpfile.write("{} {} {} {}\n".format(self.host, item_key, timestamp, value))
            tmpfile.flush()
            tmpfile_name = tmpfile.name

        cmd = [
            self.zabbix_sender,
            '-T',
            '-z', self.server,
            '-i', tmpfile_name
        ]

        output = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
        os.remove(tmpfile_name)
