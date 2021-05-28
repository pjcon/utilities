"""Script to query AMS subscriptions and show the backlog of messages."""

# References used in the creation of this script.
# https://realpython.com/python-string-formatting/
# https://stackoverflow.com/a/12965254/1442342

from __future__ import print_function

import json
import urllib

# An admin token for the AMS project needs to be provided.
TOKEN = ''
URL_TEMPLATE = ('https://msg.argo.grnet.gr/v1/projects/accounting/'
                'subscriptions/{sub}:offsets?key={token}')
TYPES = ('grid', 'cloud', 'storage')

print("Subscription     \tBacklog")
print("="*31)
for type in TYPES:
    for service in ('repository', 'portal'):
        sub = service + '-' + type
        url = URL_TEMPLATE.format(sub=sub, token=TOKEN)
        # print(url)
        response = urllib.urlopen(url)
        data = json.loads(response.read())
        # print(data)
        # print(data['current'], data['max'], data['max'] - data['current'])
        print(sub, data['max'] - data['current'], sep='     \t')
