#!/usr/bin/env python3
# coding: utf-8
"""Module for loading json messages from a tar archive into a dirq directory

Author: Connor Pettitt
Contact: connor.pettitt@stfc.ac.uk
"""

import argparse
import json
import tarfile

from dirq import queue

schema = {"body": "string", "signer": "string",
          "empaid": "string?", "error": "string?"}

def main(messages_tar=None, path='./'):
    """ DOC
    messages_tar: filename
        Existing filename to tar archive containing files with message bodies.

    path: pathname
        Pathname to directory (created if non-existant) where dirq will place messages.
    """
    message_strings = []

    try:
        with tarfile.open(name=messages_tar, mode='r', fileobj=None, bufsize=10240) as tar:
            for tfile in tar:
                file = tar.extractfile(tfile)
                if file is not None:
                    message_strings.append(file.read())
    except Exception as e:
        print(e)
        print(f'Not a valid tar archive: {messages_tar}')
        exit(1)

    message_bodies = []

    print('Loading json strings.', end='')
    for mi, mstr in enumerate(message_strings):        
        # TODO check message type
        # TODO load apel format messages

        #try:
        body = json.loads(mstr)
        message_bodies.append(body)
        #except:
            #print('Failed to load message:', mstr)
            #print('')


        if mi % 100 == 0: print('.', end='')
    print('')
        
    Q = queue.Queue(path, schema=schema)

    print('Pushing messages into queue.', end='')
    for mi, body in enumerate(message_bodies):
        body = json.dumps(body).replace("'",'"')
        message = {'body':body, 'signer':'test signer', 'empaid':'', 'error':''}

        #try:
        Q.add(message)
        #except:
            #print(f'Failed to add message to queue: {message}')

        if mi % 100 == 0: print('.', end='')
    print('')


if __name__ == '__main__':
    import os

    parser = argparse.ArgumentParser(description='Get input and output pathnames.')

    parser.add_argument('-m/--messages', dest='messages_tar', required=True, 
                        help='Supply a filename for input messages.')
    parser.add_argument('-p/--path', dest='path', default='./', 
                        help='Supply a path for Queue output (default: ./).')

    args = parser.parse_args()

    messages_tar = args.messages_tar

    if not os.path.exists(messages_tar):
        print(f'File does not exist: {messages_tar}')
        exit(1)

    try:
        open(messages_tar, 'r')
    except:
        print(f'Not a readable file: {messages_tar}')
        exit(1)

    path = args.path

    main(messages_tar=messages_tar, path=path)