'''
a wrapper to google cloud storage.

Copyright 2015, Institute for Systems Biology.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
'''
from googleapiclient import discovery, http, errors
import json
from multiprocessing import Lock
from oauth2client.client import GoogleCredentials
import requests
import time
import io



STORAGE_SCOPES = [
    'https://www.googleapis.com/auth/devstorage.read_only',
    'https://www.googleapis.com/auth/devstorage.read_write',
    'https://www.googleapis.com/auth/devstorage.full_control'
]


def get_storage_resource():
    try:
        credentials = GoogleCredentials.get_application_default()
    except:
        # todo: replace filename with variable
        credentials = GoogleCredentials.from_stream("privatekey.json").create_scoped(STORAGE_SCOPES)
    return discovery.build('storage', 'v1', credentials=credentials)

# value to delay resubmitting
back_off = 0

lock = Lock()


def upload_file(file_path, bucket_name, key_name, log, gcs_metadata):
    '''
    :param file_path: archive_path + file_name e.g. tmp/nationwidechildrens.org_BLCA.bio.Level_1.252.53.0/nationwidechildrens.org_clinical.TCGA-BT-A42E.xml
    :param bucket_name: config['buckets']['open'] which is "<isb-cgc-bucket name>"
    :param key_name: example - 'tcga'/tumor_type/'bio'/'Level_1'/file_name nb: 'bio' and 'Level_1' happens when called by parse_bio
    :param log: example - logging.getLogger('2015_06_15_logs/' + tumor_type + '_bio') when called by parse_bio
    :param gcs_metadata: example -  {"ClinicalfileName": "nationwidechildrens.org_control.TCGA-07-0227.xml",
                                     "ParticipantBarcode": "TCGA-AB-2802",
                                     "bcr": "Washington University",
                                     "ParticipantUUID": "b93cb62a-a7dc-406d-8482-6b51a92ea3c3",
                                     "TSSCode": "AB"} .. and many more clinical fields
    '''
    global back_off
    for attempt in range(1, 4):
        try:
            __attempt_upload(file_path, bucket_name, key_name, log, gcs_metadata)
            back_off *= .9
            if back_off < .006:
                back_off = 0
#             log.info('\tcompleted upload %s' % (key_name))
            return
        except requests.exceptions.ConnectionError:
            with lock:
                # request failed, trigger backoff
                if back_off == 0:
                    back_off = .005
                else:
                    back_off = min(1, back_off * 1.15)
            log.warning('\tattempt %s had connection error.  backoff at: %s' % (attempt, back_off))
        except Exception as e:
            log.exception('\tproblem uploading %s' % key_name)
            raise e
    log.error('\tfailed to upload %s' % key_name)
    raise ValueError('\tcould not load %s' % key_name)


def __attempt_upload(file_path, bucket_name, key_name, log, gcs_metadata):
    '''
    :param file_path: archive_path + file_name e.g. tmp/nationwidechildrens.org_BLCA.bio.Level_1.252.53.0/nationwidechildrens.org_clinical.TCGA-BT-A42E.xml
    :param bucket_name: config['buckets']['open'] which is "isb-cgc-open"
    :param key_name: example - 'tcga'/tumor_type/'bio'/'Level_1'/file_name --  'bio' and 'Level_1' happens when called by parse_bio
    :param log: example - logging.getLogger('2015_06_15_logs/' + tumor_type + '_bio') when called by parse_bio

    '''
    time.sleep(back_off)

    storage_service = get_storage_resource()
    # todo: restrict metadata to aliquot barcode, UUID (if there is one), platform, file type (CEL, DNAseq bam, RNAseq bam, etc)
    body = {"metadata": gcs_metadata}
    req = storage_service.objects().insert(bucket=bucket_name, name=key_name, media_body=file_path, body=body)
    try:
        result = req.execute()
        # example result (for log)
        # {u'bucket': u'isb-cgc-dev',
        #  u'contentType': u'text/xml',
        #  u'crc32c': u'PJ6y3w==',
        #  u'etag': u'CPDmppqeyMgCEAE=',
        #  u'generation': u'1445041265030000',
        #  u'id': u'isb-cgc-dev/kelly/test.xml/1445041265030000',
        #  u'kind': u'storage#object',
        #  u'md5Hash': u'pfKBlWV3Y0pLvlBZ7bmVMg==',
        #  u'mediaLink': u'https://www.googleapis.com/download/storage/v1/b/isb-cgc-dev/o/kelly%2Ftest.xml?generation=1445041265030000&alt=media',
        #  u'metadata': {u'a_key': u'a_value'},
        #  u'metageneration': u'1',
        #  u'name': u'kelly/test.xml',
        #  u'owner': {u'entity': u'user-00b4903a97ac192d9191975485aad901c824cd2e10a19f255819a55b5951379a',
        #             u'entityId': u'00b4903a97ac192d9191975485aad901c824cd2e10a19f255819a55b5951379a'},
        #  u'selfLink': u'https://www.googleapis.com/storage/v1/b/isb-cgc-dev/o/kelly%2Ftest.xml',
        #  u'size': u'58021',
        #  u'storageClass': u'STANDARD',
        #  u'timeCreated': u'2015-10-17T00:21:05.028Z',
        #  u'updated': u'2015-10-17T00:21:05.028Z'}

        # todo: log result if necessary
    except errors.HttpError as e:
        log.error(e)

def get_bucket_contents(bucket_name, prefix = None, log = None):
    filename2fileinfo = {}
    storage_service = get_storage_resource()
    fields_to_return = 'nextPageToken,items(kind, name, updated, size, metadata(my-key))'
    req = storage_service.objects().list(bucket=bucket_name, fields=fields_to_return)
    while req:
        resp = req.execute()
        for item in req['items']:
            filename2fileinfo[item['name']] = item
        req = storage_service.objects().list_next(req, resp)
    return filename2fileinfo

