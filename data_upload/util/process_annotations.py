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
*Redaction:Tumor tissue origin incorrect    1
*Redaction:Tumor type incorrect    2
*Redaction:Genotype mismatch    3
*Redaction:Subject withdrew consent    4
*Redaction:Subject identity unknown    5
Notification:Prior malignancy    6
Notification:Neoadjuvant therapy    7
Notification:Qualification metrics changed    8
Notification:Pathology outside specification    9
Notification:Molecular analysis outside specification    10
Notification:Duplicate item    11
Notification:Sample compromised    13
Notification:Clinical data insufficient    14
*Notification:Item does not meet study protocol    15
Notification:Item in special subset    17
Notification:Qualified in error    18
Notification:Item is noncanonical    21
Notification:New notification type    22
Observation:Tumor class but appears normal    23
Observation:Normal class but appears diseased    24
Observation:Item may not meet study protocol    25
Observation:New observation type    26
Redaction:Duplicate case    27
CenterNotification:Center QC failed    28
*CenterNotification:Item flagged DNU    29
Observation:General    30
Permanently missing item or object    36
Notification:WGA Failure    181
Normal tissue origin incorrect    35
Redaction:Administrative Compliance    37
*Notification:History of unacceptable prior treatment related to a prior/other malignancy    201
Notification:History of acceptable prior treatment related to a prior/other malignancy    202
Notification:Case submitted is found to be a recurrence after submission    203
Notification:Synchronous malignancy    204

*indicates do not include
'''
import json
import logging
import urllib

def process_annotations(config, log_name):
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
    log.info('\tfinish read annotations')
    
    exclude_annotation_catagories = [1,2,3,4,5,15,29,201]
    barcode2annotation = {}
    count = 0
    count_bad = 0
    log.info('\tstart check annotations')
    for annotation in annotations:
        if 0 == count % 2048:
            log.info('\t\tchecked %s annotations' % (count))
        count += 1
        annotationCategory = annotation['annotationCategory']
        if annotationCategory['categoryId'] in exclude_annotation_catagories:
            if not barcode2annotation.has_key(annotation['items'][0]['item']):
                count_bad += 1
            annotations = barcode2annotation.setdefault(annotation['items'][0]['item'], {})
# AnnotationCategory
# AnnotationClassification
            annotationCategories = annotations.setdefault('AnnotationCategory', [])
            if annotationCategory['categoryName'] not in annotationCategories:
                annotationCategories += [annotationCategory['categoryName']]
            annotationClassification = annotations.setdefault('AnnotationClassification', [])
            if annotationCategory['annotationClassification']['annotationClassificationName'] not in annotationClassification:
                annotationClassification += [annotationCategory['annotationClassification']['annotationClassificationName']]

    log.info('\tfinished processing annotations')
    return barcode2annotation
