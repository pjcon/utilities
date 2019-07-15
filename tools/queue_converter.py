"""Tool to convert unloaded messages to loadable ones."""

from dirq.QueueSimple import QueueSimple
from dirq.queue import Queue

inpath = '/root/iris/messages'
outpath = '/root/iris/messages/incoming'

OUTQSCHEMA = {"body": "string", "signer": "string",
              "empaid": "string?", "error": "string?"}

inq = QueueSimple(inpath)
outq = Queue(outpath, schema=OUTQSCHEMA)

for name in inq:
    if not inq.lock(name):
        continue
    data = inq.get(name)
    outq.add({'body': data, 'signer': 'iris', 'empaid': 'local'})
    inq.remove(name)
