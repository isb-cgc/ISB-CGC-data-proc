'''
Created on Mar 27, 2015

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

@author: michael

json return layout
{
    "dccAnnotation":[ {
        "id":26882,
        "dateCreated":"2015-03-06T14:42:56-05:00",
        "createdBy":"LeraasK",
        "status":"Approved",
        "annotationCategory": {
            "categoryId":25,
            "categoryName":"Item may not meet study protocol",
            "annotationClassification": {
                "annotationClassificationId":1,
                "annotationClassificationName":"Observation"
            }
        },
        "items":[ {
            "item":"TCGA-HT-7483",
            "uuid":"183dd089-e932-4be2-b252-0e8572e7da4e",
            "itemType": {
                "itemTypeId":3,
                "itemTypeName":"Patient"
            },
            "disease": {
                "diseaseId":21,
                "abbreviation":"LGG",
                "description":"Brain Lower Grade Glioma"
            },
            "id":26338
        } ],
        "notes":[ {
            "noteId":26427,
            "noteText":"TSS confirmed that submitted tumor is a recurrence and patient had 2 prior resections before tumor submitted to BCR. Submitted tumor was in same tumor bed as primary. The patient had no prior chemo/radiation treatment. ",
            "addedBy":"LeraasK",
            "dateAdded":"2015-03-06T14:42:56-05:00"
            "dateEdited":"2015-03-07T14:42:56-05:00"
        } ],
        "approved":true,
        "rescinded":false
    }
    ...
    ]
}

Annotation Classification    Id
Observation    1
CenterNotification    2
Notification    3
Redaction    5

Annotation Category    Id
annotationClassification    annotationCategoryName    annotationCategoryId
CenterNotification:Center QC failed    28
CenterNotification:Item Flagged Low Quality    465
CenterNotification:Item flagged DNU    29
Notification:Acceptable treatment for TCGA tumor    355
Notification:Alternate sample pipeline    205
Notification    BCR Notification:    38
Notification:Barcode incorrect    243
Notification:Case submitted is found to be a recurrence after submission    203
Notification:Duplicate item    11
Notification:History of acceptable prior treatment related to a prior/other malignancy    202
Notification:History of unacceptable prior treatment related to a prior/other malignancy    201
Notification:Item does not meet study protocol    15
Notification:Item in special subset    17
Notification:Item is noncanonical    21
Notification    Molecular analysis outside specification:    10
Notification:Neoadjuvant therapy    7
Notification:Normal tissue origin incorrect    35
Notification:Pathology outside specification    9
Notification:Permanently missing item or object    36
Notification:Prior malignancy    6
Notification    Qualification:metrics changed    8
Notification:Qualified in error    18
Notification:Sample compromised    13
Notification:Synchronous malignancy    204
Notification:WGA Failure    181
Observation:General    30
Observation:Item may not meet study protocol    25
Observation:Normal class but appears diseased    24
Observation:Tumor class but appears normal    23
*Redaction:Administrative Compliance    37
*Redaction:Biospecimen identity unknown    242
*Redaction:Duplicate case    27
*Redaction:Genotype mismatch    3
*Redaction:Inadvertently shipped    241
*Redaction:Subject identity unknown    5
*Redaction:Subject withdrew consent    4
*Redaction:Tumor tissue origin incorrect    1
*Redaction:Tumor type incorrect    2

*indicates do not include
'''
import json
import logging
import pprint
import urllib

import isbcgc_cloudsql_annotation_model
import isbcgc_cloudsql_annotation_association_model
from util import post_run_file

def associate_metadata2annotation(config, log):
    # now create the annotation association tables and save the associations
    isbcgc_cloudsql_annotation_association_model.ISBCGC_database_helper.initialize(config, log)

    associate_statements = [
        "insert into metadata_annotation2data " \
            "(metadata_annotation_id, metadata_data_id) " \
        "select a.metadata_annotation_id, s.metadata_data_id " \
        "from metadata_annotation a join metadata_data s on " \
            "0 < instr(s.aliquotbarcode, a.itembarcode) ",
        "insert into metadata_annotation2clinical " \
            "(metadata_annotation_id, metadata_clinical_id) " \
        "select a.metadata_annotation_id, s.metadata_clinical_id " \
        "from metadata_annotation a join metadata_clinical s on " \
            "s.participantbarcode = a.itembarcode ",
        "insert into metadata_annotation2biospecimen "  \
            "(metadata_annotation_id, metadata_biospecimen_id) " \
        "select a.metadata_annotation_id, s.metadata_biospecimen_id " \
        "from metadata_annotation a join metadata_biospecimen s on " \
            "0 < instr(s.samplebarcode, a.itembarcode) ",
        "insert into metadata_annotation2samples " \
            "(metadata_annotation_id, metadata_samples_id) " \
        "select a.metadata_annotation_id, s.metadata_samples_id " \
        "from metadata_annotation a join metadata_samples s on " \
            "0 < instr(s.samplebarcode, a.itembarcode) "
    ]
    
    for statement in associate_statements:
        isbcgc_cloudsql_annotation_association_model.ISBCGC_database_helper.update(config, statement, log, [[]], True)

def parse_derived(annotation, derived_keys, length, log):
    derived = parse_item(annotation, derived_keys, log)
    fields = derived.split('-')
    if not 'TCGA' == fields[0] or 2 >= len(fields):
        # unexpected value!
        log.exception('unexpected derived value: %s' % (derived))
        raise ValueError('unexpected derived value: %s' % (derived))
    if len(fields) >= length:
        return '-'.join(fields[:length])
    else:
        return None

def parse_item(annotation, keys, log):
    try:
        retval = None
        if 'derived' == keys[0]:
            retval = parse_derived(annotation, keys[2:], keys[1], log)
        elif 'optional' == keys[0]:
            if 4 != len(keys):
                # need to add new case!
                log.exception('unexpected optional specification: %s:%s' % (annotation, keys))
                raise ValueError('unexpected optional specification: %s:%s' % (annotation, keys))
            if keys[1] in annotation and keys[3] in annotation[keys[1]][keys[2]]:
                retval = annotation[keys[1]][keys[2]][keys[3]]
        elif 1 == len(keys):
            retval = annotation[keys[0]]
        elif 2 == len(keys):
            retval = annotation[keys[0]][keys[1]]
        elif 3 == len(keys):
            retval = annotation[keys[0]][keys[1]][keys[2]]
        elif 4 == len(keys):
            retval = annotation[keys[0]][keys[1]][keys[2]][keys[3]]
        else:
            # need to add new case!
            log.exception('unexpected specification: %s:%s' % (annotation, keys))
            raise ValueError('unexpected specification: %s:%s' % (annotation, keys))
        if keys[-1].startswith('date') and retval:
            retval = '-'.join(retval.split('-')[:3]).replace('T', ' ')
        return retval
    except Exception as e:
        log.exception('error parsing annotation: %s:%s' % (annotation, keys))
        raise e
        
def parse_annotation(ann_config, annotation, log):
    annotationList = [None] * len(ann_config)
    keys = ann_config.keys()
    keys.sort()
    for index, key in enumerate(keys):
        annotationList[index] = parse_item(annotation, ann_config[key], log)
    
    return [annotationList]

def process_annotations(config, run_dir, log_name):
    log = logging.getLogger(log_name)
    log.info('start processing annotations')
    
    # getting a memory error using the request package
    # annotations = json.loads(util.getURLData(annotationURL, 'annotations', log))
    # but urllib does work!!!
    log.info('\tstart fetch annotations')
    try:
        response = urllib.urlopen(config['downloads']['annotations'])
    except Exception as e:
        log.exception('problem fetching annotations')
        if 'test' == config['mode']:
            log.warning('returning empty map for testing purposes')
            return {}
        else:
            raise e
    log.info('\tfinish fetch annotations')

    log.info('\tstart read annotations')
    annotations = json.loads(response.read())['dccAnnotation']
    post_run_file(run_dir, 'dcc_annotations.json', pprint.pformat(annotations, width = 300))
    log.info('\tfinish read annotations')
    
    exclude_annotation_catagories = config['exclude_annotation_catagories']
    ann_config = config['annotation_mapping']
    barcode2annotation = {}
    annotation_lists = []
    count = 0
    count_bad = 0
    log.info('\tstart check annotations')
    for annotation in annotations:
        if 0 == count % 2048:
            log.info('\t\tchecked %s annotations' % (count))
        count += 1
        
        # sanity check assumptions
        try:
            if annotation['rescinded'] or 'Approved' != annotation['status']:
                log.info('skipping rescinded or non-approved annotation:%s' % (annotation))
                continue
            
            if 1 != len(annotation['items']):
                log.exception('items has unexpected length: %s' % (annotation))
                raise ValueError('items has unexpected length: %s' % (annotation))
            if 'notes' in annotation and 1 != len(annotation['notes']):
                log.warning('\t\tfound multiple notes: \n\t\t\t%s' % ('\n\t\t\t'.join(notes['noteText'].strip() for notes in annotation['notes'])))
            
            annotation_lists += parse_annotation(ann_config, annotation, log)
            annotationCategory = annotation['annotationCategory']
            if annotationCategory['categoryId'] in exclude_annotation_catagories:
                if not barcode2annotation.has_key(annotation['items'][0]['item']):
                    count_bad += 1
                annotations = barcode2annotation.setdefault(annotation['items'][0]['item'], {})
                annotationCategories = annotations.setdefault('AnnotationCategory', [])
                if annotationCategory['categoryName'] not in annotationCategories:
                    annotationCategories += [annotationCategory['categoryName']]
                annotationClassification = annotations.setdefault('AnnotationClassification', [])
                if annotationCategory['annotationClassification']['annotationClassificationName'] not in annotationClassification:
                    annotationClassification += [annotationCategory['annotationClassification']['annotationClassificationName']]
        except Exception as e:
            log.exception('exception occurred on line %s for %s' % (count, annotation))
            raise e

    # now create the annotation table and save to the cloudsql annotation table
    isbcgc_cloudsql_annotation_model.ISBCGC_database_helper.initialize(config, log)
    isbcgc_cloudsql_annotation_model.ISBCGC_database_helper.column_insert(config, annotation_lists, "metadata_annotation", sorted(ann_config.keys()), log)

    log.info('\tfinished processing annotations')
    return barcode2annotation
