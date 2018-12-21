# A python script to upload documents to an Elastic index

import elasticsearch as es
import logging
import base64
from os import listdir
from os.path import isfile, join
import hashlib

def connect_elasticsearch():
    _es = None
    _es = es.Elasticsearch([{'host': '172.16.1.239', 'port': 9200}])
    if _es.ping():
        print('Yay connected!')
    else:
        print('No connection...')
    return _es

def convert_doc_b64(docpath):
    with open(docpath) as f:
        b64repres = base64.b64encode(f.read())
    return b64repres

if __name__ == '__main__':
    logging.basicConfig(level=logging.ERROR)
    es_conn = connect_elasticsearch()


    # get all documents in folder Rapported decrypted
    path = 'Rapporten decrypted/nl/'
    files = [join(path, f) for f in listdir(path) if isfile(join(path, f))]

    # loop over files, convert to b64 and put to elasticsearch
    for file in files:

        filehash = hashlib.md5(file).hexdigest()
        doc_b64 = convert_doc_b64(file)

        # format body
        doc_body = {
            "data": doc_b64
        }

        # put to Elasticsearch
        es_conn.index(
            index='ovv_rapporten',
            doc_type='documenten',
            body=doc_body,
            id = filehash,
            pipeline='documents'
        )

        # check if creation is succesful
        #es_conn.get(index='ovv_rapporten', doc_type='documenten', id=filehash)
