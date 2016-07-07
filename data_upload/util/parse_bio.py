'''
Created on Mar 26, 2015

Code for parsing the TCGA bio files: clinical, biospecimen auxiliary,
ssf, and omf

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
'''
from collections import defaultdict
import logging
from lxml import etree
import os
import re
import shutil
from threading import Lock

import util

clinical_pat = re.compile("^.*clinical.*.xml$")
auxiliary_pat = re.compile("^.*auxiliary.*.xml$")
ssf_pat = re.compile("^.*ssf.*.xml$")
omf_pat = re.compile("^.*omf.*.xml$")
biospecimen_pat = re.compile("^.*biospecimen.*.xml$")
blank_elements = re.compile("^\\n\s*$")

lock = Lock()

class Calculate:
    '''
    class to encapsulate calculating derived field values
    '''
    def calculateBMI(self, (weight, height, log)):
        '''
        use the weight and height to calculate the BMI
        
        parameters:
            weight: weight of the participant
            height: height of the participant
            log: logger to log any messages
        '''
        return float(weight) / pow(float(height)/100, 2)

    def enforce_vitalstatus_rules(self, filtered_dict, log):
        '''
            for all patients who are "Alive", we should have (ie force this to be true):
            * days_to_last_known_alive = days_to_last_followup (hopefully not NA)
            * days_to_death = NA
    
            for all patients who are "Dead", we should have
            * days_to_last_known_alive = days_to_death (and not NA)
                * if we do not have a days_to_death value, then I would almost rather we revert the vital_status to Alive)
            * days_to_last_followup = NA
        
            parameters:
                filtered_dict: the dictionary to use to check for the key/values to enforce vital_status
                log: logger to log any messages
        '''
        days_to_last_followup = filtered_dict.get('days_to_last_followup')
        days_to_death = filtered_dict.get('days_to_death') 
        vital_status = filtered_dict.get('vital_status')
        days_to_last_known_alive = None
        
        if vital_status == 'Dead':
            if days_to_death:
                days_to_last_known_alive = days_to_death
                days_to_last_followup = None
            else:
                log.info("No days_to_death in the file or is NULL. participant: %s" % filtered_dict['ParticipantBarcode']) 
                if days_to_last_followup or days_to_last_known_alive:
                    vital_status = 'Alive'
                  
        
        if vital_status == 'Alive':
            if days_to_last_followup:
                days_to_last_known_alive = days_to_last_followup
            else:
                log.info("No days_to_last_followup in the file or is NULL. participant: %s" % filtered_dict['ParticipantBarcode'])
            days_to_death = None
    
        filtered_dict['days_to_last_followup'] = days_to_last_followup
        filtered_dict['days_to_death'] = days_to_death
        filtered_dict['days_to_last_known_alive'] = days_to_last_known_alive
        filtered_dict['vital_status'] = vital_status
    
calculate = Calculate()

def upload_file(config, file_path, key_name, log):
    '''
    upload the file to GCS
    
    parameters:
        file_path: path to the local file
        key_name: GCS object key to use
        log: logger to log any messages
    '''
    bucket_name = config['buckets']['open']
    if config['upload_files']:
        log.info('\tuploading %s' % (key_name))
        util.upload_file(config, file_path, bucket_name, key_name, log)

def upload_bio_file(config, archive_path, file_name, study, log):
    '''
    upload the bio file to GCS

    parameters:
        archive_path: path to the files for the archive
        file_name: name of the file to upload
        study: study name to use as part of the GCS object key
        log: logger to log any messages
    '''
    key_name = '/%s/%s/%s/%s/%s' % ('tcga', study, 'bio', 'Level_1', file_name)
    upload_file(config, archive_path + file_name, key_name, log)

def filter_data(log, data, fields):
    '''
    from the data map, filter the desired fields specified by the keys of field
    with the new key from the value of the field values
    if a field is a calculated field, create it from the appropriate fields by calling
    the calculate method for it

    parameters:
        log: logger to log any messages
        data: parsed map of element to value
        fields: map of field names to get from data and the new key to use for the value in
        the returned map
    '''
    filtered_data = {}
    for barcode in data:
        filtered_dict = {}
        for field in fields:
            if 'calculate' == field:
                continue
            new_keys = fields.get(field)
            if field in data[barcode] and data[barcode][field] not in ['', None]:
                for new_key in new_keys.split(','):
                    if new_key in filtered_dict and filtered_dict[new_key] != data[barcode][field] and \
                      not (filtered_dict[new_key].lower() in data[barcode][field].lower() or data[barcode][field].lower() in filtered_dict[new_key].lower()):
                        log.warning('%s--values not equal between bio files for %s: %s != %s' % (barcode, new_key, filtered_dict[new_key], data[barcode][field]))
                        continue
                    filtered_dict[new_key] = data[barcode][field]
        if 'calculate' in fields:
            calculate_dict = fields['calculate']
            for function_info, new_key in calculate_dict.iteritems():
                function_fields = function_info.split(':')
                function = function_fields[0]
                if 2 == len(function_fields) and 'filtered_dict' == function_fields[1]:
                    getattr(calculate, function)(filtered_dict, log)
                else:
                    args = []
                    empty = False
                    for arg_name in function_fields[1:-1]:
                        if arg_name not in filtered_dict or not filtered_dict[arg_name]:
                            empty = True
                            break
                        args += [filtered_dict[arg_name]]
                    if empty:
                        continue
                    args += [log]
                    filtered_dict[new_key] = function_fields[-1] % (getattr(calculate, function)(args))
        filtered_data[barcode] = filtered_dict
    return filtered_data

#--------------------------------------
# convert a XML node into dict
#--------------------------------------
def elem2dict(node, append_name):
    '''
    convert an lxml.etree node tree into a dict.

    parameters:
        mode: the start node
        append_name: name to append to the element key
    '''
    d = {}
    for e in node.findall('*[@procurement_status="Completed"]'):
        if not e.text:
            continue
        key = e.tag.split('}')[1] if '}' in e.tag else e.tag
        value = e.text.strip() 
        d[append_name + ":" + key] = value
        if key in ('batch_number'):
            d[key] = value
      
    return d

# --------------------------------------
# Average slides block elements; 
# collect elements starting with percent
# --------------------------------------
def average_slides(slides):
    '''
    take the value for each field for the slide elements and produce the average

    parameters:
        slides: the set of slide values to average
    '''
    slide_values = []

#   for slide in slides.findall("bio:slide", namespaces=root.nsmap):
    for slide in slides:
        slide_values.append(elem2dict(slide, "slide"))

    dd = defaultdict(list)
    for d in slide_values: 
        for key, value in d.iteritems():
            if key.startswith("slide:percent_"):
                # this will crash if the value is not int
                # the values must be INT ( no exceptions)
                dd[key].append(int(value))

    # take average 
    average_slide_values = {}  
    for key, value in dd.iteritems():
        name = str(key).split(":")
        avg_key = name[0] + ":avg_" + name[1]
        min_key = name[0] + ":min_" + name[1]
        max_key = name[0] + ":max_" + name[1]
        average_slide_values[avg_key] =  str(sum(value) / float(len(value)))
        average_slide_values[max_key] =  str(max(value))
        average_slide_values[min_key] =  str(min(value))
    return average_slide_values

# -------------------------------------
# TCGA files have many namespaces, 
#  we need to get all of them correctly
# -------------------------------------
def parse_biospecimen(biospecimen_file, log, biospecimen_uuid2field2value, key_field, exclude_samples, sample_code2letter, sample_code2type):
    ''' 
    parse biospecimen files and generate a dictionary with element to value

    parameters:
        biospecimen_file: the biospecimen xml file to parse
        log: logger to log any messages
        biospecimen_uuid2field2value: the map to add the parsed element to value map by the specified key_field
        key_field: the field to use as the key for the biospecimen_uuid2field2value map
        sample_code2letter: map to translate the numeric sample code to the alpha sample code
        sample_code2type: map to translate the numeric sample code to the sample type
    '''
    log.info('\tprocessing %s' % (biospecimen_file))

    # get the XML tree (lxml)
    tree = etree.parse(biospecimen_file)
    root = tree.getroot() #this is the root; we can use it to find elements

    #----------------------------------------------------------
    # ADMIN BLOCK
    #----------------------------------------------------------
    #----------------------------------------------------------
    # tcga data is divided into admin and patient blocks
    # We are getting the admin elements here
    #-----------------------------------------------------------
    admin_features = {}
    # get admin block elements
    admin_element = root.find('.//admin:admin', namespaces=root.nsmap)
    # we are not using elem2dict here beacuse we want one extra level to parse
    for child in admin_element:
        for child_all in child.findall('*'):
            # Select all child elements - not going too deep, just one level
            if child_all.text:
                admin_features["admin:" + child_all.tag.split("}")[1]] = child_all.text.strip()
        if child.text:
            admin_features["admin:" + child.tag.split("}")[1]] = child.text.strip()
    #stripping the batch number to just the first portion 
    admin_features["batch_number"] = admin_features["admin:batch_number"].split(".")[0]
    admin_features["admin:batch_number"] = admin_features["admin:batch_number"].split(".")[0]
  
  
    #-----------------------------------------------------------
    # PATIENT BLOCK
    #-----------------------------------------------------------
    patient_element = root.find('.//bio:patient', namespaces=root.nsmap)
    patient_features = elem2dict(patient_element, "patient") # 1

    # there are some patients with a "null" Project, issue warning and do not include     
    if 'admin:project_code' not in admin_features:
        log.warning("No project code found for the file %s, excluding" % biospecimen_file)
        exclude_samples.add(admin_features['sample:bcr_sample_barcode'])
        return

    # samples
    samples_element = patient_element.find('bio:samples', namespaces=root.nsmap)
    for sample in samples_element.findall("bio:sample", namespaces=root.nsmap):
        sample_features = elem2dict(sample, "sample") # 2
     
        # portions
        portions_element = sample.find('bio:portions', namespaces=root.nsmap)
        portions = (portions_element.findall("bio:portion", namespaces=root.nsmap))

        # slides
        slides_element = portions_element.findall('.//bio:slide', namespaces=root.nsmap)
        slide_features =  (average_slides(slides_element)) #3 ( average slides)

        sample_features["sample:num_portions"] = str(len(portions))
        sample_features["sample:num_slides"] = str(len(slides_element))

        # add sample type
        sample_features["sample:SampleTypeCode"] = sample_features['sample:bcr_sample_barcode'].split("-")[3][0:2]
        sample_features["sample:SampleType"] = sample_code2type[sample_features['sample:bcr_sample_barcode'].split("-")[3][0:2]]
        sample_features["sample:SampleTypeLetterCode"] = sample_code2letter[sample_features['sample:bcr_sample_barcode'].split("-")[3][0:2]]

        features = dict(list(sample_features.items()) + list(slide_features.items())
                        + list(patient_features.items()) + list(admin_features.items()))

        # FFPE samples should be excluded 
#         if 'sample:is_ffpe' in features and features['sample:is_ffpe'] == 'YES':
#             print(("WARNING: FFPE samples should be excluded. Skipped file: ", biospecimen_file))
#             return None
        biospecimen_uuid2field2value[features[key_field]] = features

def merge_maps_master_other(master_contents, other_contents, other_tag):
    '''
    take the key/value from other_conents and merge into master_contents

    parameters:
        master_contents: the map to copy into
        other_contents: the map to copy from
        other_tag: string to label any error messages
    '''
    data = {}
    for key in master_contents:
        if key in other_contents:
            master_data = master_contents[key].items()
            other_data = other_contents[key].items()
            if master_contents[key]['batch_number'] != other_contents[key]['batch_number']:
                raise Exception("batch_numbers between the master map and %s data must be same" % (other_tag))
            data[key] = dict(other_data + master_data)
        else:
            data[key] = master_contents[key]
    return data

#-------------------------------------------------------------------------------
# get all elements - recursively,
#   actually, this is the only function we need to parse the XML
# Same function is used in the auxiliary parsing
#-------------------------------------------------------------------------------
def elem2dict_clinical(node, xml_dict, nodes_to_avoid, log, max_level = 4, prefix = '', level=0):
    '''
    recursively parse through the nodes
    
    parameters:
        node: the node to start from
        xml_dict: where to put the results of the parse <element>: <text>
        nodes_to_avoid: list of nodes to skip in the recursion
        max_level: the maximum depth to recurse from node
        prefix: what to append in front of the element key
        level: the current level of recursion 
    '''
    # TCGA data is some manually entered, so we will see some non-ascii characters
    # this should break if it has any non-ascii characters
    try:
        node_text = node.text
        str(node.text)
    except (UnicodeEncodeError):
        node_text = node.text.encode("utf-8", 'ignore')
    except:
        node_text = re.sub(r'[^\x00-\x7F]+',' ', node.text)
 
    node_tag = node.tag.split('}')[1] if '}' in node.tag else node.tag
   
    # here we do not want to replace upper level elements with lower levels
    if (not blank_elements.match(str(node_text))) and node_text:
        if node_tag in xml_dict:
            # verify that the value is the same or log a warning
            # if node_text.strip().lower() != xml_dict[node_tag].lower():
            #    log.warning('values for repeated %s don\'t match: %s != %s' % (node_tag, xml_dict[node_tag], node_text))
            pass
        else:
            if prefix:
                xml_dict[prefix + node_tag] = node_text.strip()
            else:
                xml_dict[node_tag] = node_text.strip()
            if prefix and node_tag in ('disease_code', 'batch_number', 'bcr_patient_barcode'):
                xml_dict[node_tag] = node_text.strip()
    
    # Go to only level 3
    if level == max_level:
        return xml_dict

    for child in node.iterchildren():
        if child.tag.split("}")[1] not in nodes_to_avoid:
            # recursive 
            elem2dict_clinical(child, xml_dict, nodes_to_avoid, log, max_level, prefix, level+1)
    return xml_dict


# -------------------------------------
# TCGA files have many namespaces, 
#  we need to get all of them correctly
# -------------------------------------
def parse_omf(omf_file, log, omf_barcode2field2value, key_field):
    ''' 
    parse omf files and generate a dictionary with element to value

    parameters:
        omf_file: the omf xml file to parse
        log: logger to log any messages
        omf_barcode2field2value: the map to add the parsed element to value map by the specified key_field
        key_field: the field to use as the key for the biospecimen_uuid2field2value map
    '''
    log.info('\tprocessing %s' % (omf_file))
    
    # get the XML tree (lxml)
    tree = etree.parse(omf_file)
    root = tree.getroot() #this is the root; we can use it to find elements
    
    #----------------------------------------------------------
    # ADMIN BLOCK
    # ssf data is divided into admin and patient blocks
    #-----------------------------------------------------------
    admin_features = {}
    # get admin block elements
    admin_element = root.find('.//admin:admin', namespaces=root.nsmap)
    nodes_to_avoid = []
    elem2dict_clinical(admin_element, admin_features, nodes_to_avoid, log, 10)
    admin_features["batch_number"] = admin_features["batch_number"].split(".")[0]

    #-----------------------------------------------------------
    # PATIENT BLOCK
    #-----------------------------------------------------------
    elements_to_avoid = []
    patient_element = root.find(".//omf:patient", namespaces=root.nsmap)
    patient_features = {}
    elem2dict_clinical(patient_element, patient_features, elements_to_avoid, log, 1)
    
    omfs_element = root.find(".//omf:omfs", namespaces=root.nsmap)
    for omf_element in omfs_element:
        cur_omf_features = {}
        elem2dict_clinical(omf_element, cur_omf_features, elements_to_avoid, log, 10, 'omf:')
        for cur_omf_feature in cur_omf_features:
            if cur_omf_feature in patient_features:
                patient_features[cur_omf_feature] = '%s, %s' % (patient_features[cur_omf_feature], cur_omf_features[cur_omf_feature])
            else:
                patient_features[cur_omf_feature] = cur_omf_features[cur_omf_feature]

    omf_features = dict(admin_features)
    omf_features.update(patient_features)
    omf_barcode2field2value[omf_features[key_field]] = omf_features

def parse_ssf_clinical(ssf_file, log, ssf_clinical_barcode2field2value, key_field):
    ''' 
    parse ssf files and generate a dictionary with element to value to add to the clinical metadata

    parameters:
        ssf_file: the ssf xml file to parse
        log: logger to log any messages
        ssf_barcode2field2value: the map to add the parsed element to value map by the specified key_field
        key_field: the field to use as the key for the biospecimen_uuid2field2value map
    '''
    log.info('\tprocessing %s for clinical' % (ssf_file))
    
    # get the XML tree (lxml)
    tree = etree.parse(ssf_file)
    root = tree.getroot() #this is the root; we can use it to find elements
    
    #----------------------------------------------------------
    # ADMIN BLOCK
    # ssf data is divided into admin and patient blocks
    #-----------------------------------------------------------
    admin_features = {}
    # get admin block elements
    admin_element = root.find('.//admin:admin', namespaces=root.nsmap)
    nodes_to_avoid = []
    elem2dict_clinical(admin_element, admin_features, nodes_to_avoid, log, 10)
    admin_features["batch_number"] = admin_features["batch_number"].split(".")[0]

    #-----------------------------------------------------------
    # PATIENT BLOCK
    #-----------------------------------------------------------
    elements_to_avoid = []
    patient_element = root.find(".//ssf:patient", namespaces=root.nsmap)
    patient_features = {}
    elem2dict_clinical(patient_element, patient_features, elements_to_avoid, log, 10, "clinical:")
    
    ssf_features = dict(admin_features)
    ssf_features.update(patient_features)
    ssf_clinical_barcode2field2value[ssf_features[key_field]] = ssf_features

def parse_ssf_biospecimen(ssf_file, log, ssf_sample_uuid2field2value, key_field):
    ''' 
    parse ssf files and generate a dictionary with element to value to add to the biospecimen metadata

    parameters:
        ssf_file: the ssf xml file to parse
        ssf_uuid2field2value: the map to add the parsed element to value map by the specified key_field
        key_field: the field to use as the key for the biospecimen_uuid2field2value map
    '''
    log.info('\tprocessing %s for sample' % (ssf_file))
    
    # get the XML tree (lxml)
    tree = etree.parse(ssf_file)
    root = tree.getroot() #this is the root; we can use it to find elements
    
    # get the batch number for later verification against the main biospecimen fields
    batch_element = root.find(".//admin:batch_number", namespaces=root.nsmap)
    batch_number = batch_element.text.strip().split(".")[0]
    
    elements_to_avoid = []
    tumors_element = root.find(".//ssf:tumor_samples", namespaces=root.nsmap)
    for tumor_element in tumors_element:
        cur_tumor_features = {'batch_number': batch_number}
        elem2dict_clinical(tumor_element, cur_tumor_features, elements_to_avoid, log, 10, 'sample:')
        ssf_sample_uuid2field2value[cur_tumor_features[key_field]] = cur_tumor_features

    normals_element = root.find(".//ssf:normal_controls", namespaces=root.nsmap)
    for normal_element in normals_element:
        cur_normal_features = {'batch_number': batch_number}
        elem2dict_clinical(normal_element, cur_normal_features, elements_to_avoid, log, 10, 'sample:')
        ssf_sample_uuid2field2value[cur_normal_features[key_field]] = cur_normal_features

def parse_auxiliary(auxiliary_file, log, auxiliary_barcode2field2value, key_field):
    ''' 
    parse auxiliary files and generate a dictionary with element to value

    parameters:
        auxiliary_file: the omf xml file to parse
        log: logger to log any messages
        auxiliary_barcode2field2value: the map to add the parsed element to value map by the specified key_field
        key_field: the field to use as the key for the biospecimen_uuid2field2value map
    '''
    log.info('\tprocessing %s' % (auxiliary_file))
    
    # get the XML tree (lxml)
    tree = etree.parse(auxiliary_file)
    root = tree.getroot() #this is the root; we can use it to find elements

    #----------------------------------------------------------
    # ADMIN BLOCK
    # tcga data is divided into admin and patient blocks
    #-----------------------------------------------------------
    admin_features = {}
    # get admin block elements
    admin_element = root.find('.//admin:admin', namespaces=root.nsmap)
    nodes_to_avoid = []
    admin_features = elem2dict_clinical(admin_element, admin_features, nodes_to_avoid, log)
    admin_features["batch_number"] = admin_features["batch_number"].split(".")[0]

    #-----------------------------------------------------------
    # PATIENT BLOCK
    #-----------------------------------------------------------
    elements_to_avoid = []
    patient_element = root.find(".//auxiliary:patient", namespaces=root.nsmap)
    patient_data = {}
    patient_features = elem2dict_clinical(patient_element, patient_data, elements_to_avoid, log)

    # collate data
    auxiliary_features = dict(list(patient_features.items()) + list(admin_features.items()))

    # concatenate HPV calls (hpv_call_1;hpv_call_2;hp_call_3)
    auxiliary_hpv_calls = []
    for i in range(1,4):
        hpv_call = "hpv_call_" + str(i)
        if hpv_call in auxiliary_features:
            auxiliary_hpv_calls.append(auxiliary_features[hpv_call])

    if not auxiliary_hpv_calls:
        auxiliary_features['hpv_calls'] = None
    else:
        auxiliary_features['hpv_calls'] =  ";".join(str(v) for v in auxiliary_hpv_calls if v) # dont join null/None

    auxiliary_barcode2field2value[auxiliary_features[key_field]] = auxiliary_features

def parse_clinical(clinical_file, log, clinical_barcode2field2value, key_field):
    ''' 
    parse auxiliary files and generate a dictionary with element to value

    parameters:
        clinical_file: the clinical xml file to parse
        log: logger to log any messages
        clinical_barcode2field2value: the map to add the parsed element to value map by the specified key_field
        key_field: the field to use as the key for the biospecimen_uuid2field2value map
    '''
    log.info('\tprocessing %s' % (clinical_file))
   
    # get the XML tree (lxml)
    tree = etree.parse(clinical_file)
    root = tree.getroot() #this is the root; we can use it to find elements

    #----------------------------------------------------------
    # ADMIN BLOCK
    #-----------------------------------------------------------
   
    admin_features = {}
    # gets admin block element
    admin_element = root.find('.//admin:admin', namespaces=root.nsmap)
    nodes_to_avoid = []
    admin_features = elem2dict_clinical(admin_element, admin_features, nodes_to_avoid, log)
    admin_features["batch_number"] = admin_features["batch_number"].split(".")[0]
   
    #-----------------------------------------------------------
    # PATIENT BLOCK
    #-----------------------------------------------------------
    # follow_up block (never replace these elements)
    # get the latest follow up element
    followups_element = root.find('.//' + admin_features['disease_code'].lower() + ":follow_ups", namespaces=root.nsmap)
    sequence = 0
    followups_features = {}
    elements_to_avoid = [] 
    for followup in followups_element.getchildren():
        if followup.attrib['sequence'] > sequence:
            sequence = followup.attrib['sequence']
            followups_features = {}
            followups_features = elem2dict_clinical(followup, followups_features, elements_to_avoid, log)

    # parse the patient block
    elements_to_avoid = ["follow_ups", "additional_studies", "clinical_cqcf", "drugs", "radiations"]
    patient_element = root.find('.//' + admin_features['disease_code'].lower() + ":patient", namespaces=root.nsmap)
    # we are ranking each level;
    # so that we do not replace the upper level elements with the lower ones
    patient_features = {}
    patient_features = elem2dict_clinical(patient_element, patient_features, elements_to_avoid, log)

    # there are some patients with a "null" Project, issue warning and set to TCGA     
    if 'project_code' not in admin_features:
        log.warning("\tNo project code found for the file %s" % clinical_file)
        return
  
    clinical_features = dict(list(patient_features.items()) + list(admin_features.items()) + list(followups_features.items()))

    # merge "pregnancies" and "total_number_of_pregnancies" columns ( assuming they are mutually exclusive)
    pregnancies =  clinical_features.get('pregnancies')
    total_number_of_pregnancies = clinical_features.get('total_number_of_pregnancies')
    if (not pregnancies) and  (total_number_of_pregnancies):
        if int(total_number_of_pregnancies) >= 4:
            clinical_features['pregnancies'] = '4+'
        else:
            clinical_features['pregnancies'] = total_number_of_pregnancies
    elif (pregnancies) and (total_number_of_pregnancies):
        raise Exception("\t\t'pregnancies' and 'total_number_of_pregnancies' columns are not mutually exclusive")
 
    # merge "number_of_lymphnodes_examined" and "lymph_node_examined_count" columns ( assuming they are mutually exclusive)
    number_of_lymphnodes_examined = clinical_features.get('number_of_lymphnodes_examined')
    lymph_node_examined_count = clinical_features.get('lymph_node_examined_count')
    if (not number_of_lymphnodes_examined) and  (lymph_node_examined_count):
        clinical_features['number_of_lymphnodes_examined'] = lymph_node_examined_count
    elif (number_of_lymphnodes_examined) and (lymph_node_examined_count) and number_of_lymphnodes_examined != lymph_node_examined_count:
        log.warning("\t\t'number_of_lymphnodes_examined' and 'lymph_node_examined_count' columns are not mutually exclusive:\n\t\t\t%s vs. %s" % (number_of_lymphnodes_examined, lymph_node_examined_count))

    # merge "country" and "country_of_procurement" columns ( assuming they are mutually exclusive)
    country = clinical_features.get('country')
    country_of_procurement = clinical_features.get('country_of_procurement')
    if (not country) and  (country_of_procurement):
        clinical_features['country'] = country_of_procurement
    elif (country) and (country_of_procurement) and country != country_of_procurement:
        log.warning("\t\t'country' and 'country_of_procurement' columns are not mutually exclusive:\n\t\t\t%s vs. %s" % (country, country_of_procurement))

    # merge "prior_dx" and "history_of_prior_malignancy" columns ( assuming they are mutually exclusive)
    prior_dx = clinical_features.get('prior_dx')
    history_of_prior_malignancy = clinical_features.get('history_of_prior_malignancy')
    if (not prior_dx) and  (history_of_prior_malignancy):
        clinical_features['prior_dx'] = history_of_prior_malignancy
    elif (prior_dx) and (history_of_prior_malignancy) and prior_dx != history_of_prior_malignancy:
        log.warning("\t\t'prior_dx' and 'history_of_prior_malignancy' columns are not mutually exclusive:\n\t\t\t%s vs. %s" % (prior_dx, history_of_prior_malignancy))

    clinical_barcode2field2value[clinical_features[key_field]] = clinical_features
    
def parse_file(parse_function, config, archive_path, file_name, study, upload_archive, log, type_keyfield2field2value, keyfield, *maps):
    '''
    parse and upload the file
    
    parameters:
        parse_function: function to parse the xml file
        config: the configuration map
        archive_path: path to the file being parsed
        file_name: name of the file to parse
        study: name of the TCGA study the file belongs to
        upload_archive: if true, upload the file
        log: logger to log any messages
        type_keyfield2field2value: map to add the parsed result to
        keyfield: key field to use for the type_keyfield2field2value
        maps: optional maps for translating key fields
    '''
    parse_function(archive_path + file_name, log, type_keyfield2field2value, keyfield, *maps)
    if upload_archive:
        upload_bio_file(config, archive_path, file_name, study, log)
    else:
        log.info('\tskipping upload of %s' % file_name)
    
def parse_files(config, log, files, archive_path, archive_fields, study, archive2metadata, exclude_samples, clinical_metadata, biospecimen_metadata):
    '''
    iterate through the list of filenames to parse, and if appropriate, upload them
    
    parameters:
        config: the configuration map
        log: logger to log any messages
        files: the file names to iterate through
        archive_path: path to the file being parsed
        archive_fields: list of archive_name, date of upload, and URL
        study: name of the TCGA study the files belongs to
        archive2metadata: metadata of the archive
        clinical_metadata: the return map for clinical metadata
        biospecimen_metadata: the return map for biospecimen metadata
    '''
    sample_code2letter = config['sample_code2letter']
    sample_code2type = config['sample_code2type']

    upload_archive = util.is_upload_archive(archive_fields[0], config['upload_archives'], archive2metadata) and config['upload_open']
    clinical_barcode2field2value = {}
    auxiliary_barcode2field2value = {}
    ssf_clinical_barcode2field2value = {}
    ssf_sample_uuid2field2value = {}
    omf_barcode2field2value = {}
    biospecimen_uuid2field2value = {}
    for file_name in files:
        if clinical_pat.match(file_name):
            parse_file(parse_clinical, config, archive_path, file_name, study, upload_archive, log, clinical_barcode2field2value, 'bcr_patient_barcode')
        elif auxiliary_pat.match(file_name):
            parse_file(parse_auxiliary, config, archive_path, file_name, study, upload_archive, log, auxiliary_barcode2field2value, 'bcr_patient_barcode')
        elif ssf_pat.match(file_name):
            parse_file(parse_ssf_clinical, config, archive_path, file_name, study, upload_archive, log, ssf_clinical_barcode2field2value, 'bcr_patient_barcode')
            # parsing the clinical will upload the file, set upload_archive to False to prevent the 'found <filename> in <bucket>' error
            parse_file(parse_ssf_biospecimen, config, archive_path, file_name, study, False, log, ssf_sample_uuid2field2value, 'sample:bcr_sample_uuid')
        elif biospecimen_pat.match(file_name):
            parse_file(parse_biospecimen, config, archive_path, file_name, study, upload_archive, log, biospecimen_uuid2field2value, 'sample:bcr_sample_uuid', exclude_samples, sample_code2letter, sample_code2type)
        elif omf_pat.match(file_name):
            parse_file(parse_omf, config, archive_path, file_name, study, upload_archive, log, omf_barcode2field2value, 'bcr_patient_barcode')
    
    clinical_auxiliary_barcode2field2value = merge_maps_master_other(clinical_barcode2field2value, auxiliary_barcode2field2value, 'aux')
    clinical_ssf_auxiliary_barcode2field2value = merge_maps_master_other(clinical_auxiliary_barcode2field2value, ssf_clinical_barcode2field2value, 'ssf')
    clinical_omf_ssf_auxiliary_barcode2field2value = merge_maps_master_other(clinical_ssf_auxiliary_barcode2field2value, omf_barcode2field2value, 'omf')
    clinical_auxiliary_omf_ssf_filters = config['metadata_locations']['clinical']
    clinical_auxiliary_omf_ssf_filters.update(config['metadata_locations']['auxiliary'])
    clinical_auxiliary_omf_ssf_filters.update(config['metadata_locations']['omf'])
    clinical_auxiliary_omf_ssf_filters.update(config['metadata_locations']['ssf_clinical'])
    clinical_omf_ssf_auxiliary_barcode2field2value = filter_data(log, clinical_omf_ssf_auxiliary_barcode2field2value, clinical_auxiliary_omf_ssf_filters)

    biospecimen_ssf_uuid2field2value = merge_maps_master_other(biospecimen_uuid2field2value, ssf_sample_uuid2field2value, 'ssf')
    biospecimen_filters = config['metadata_locations']['biospecimen']
    biospecimen_filters.update(config['metadata_locations']['ssf_biospecimen'])
    biospecimen_uuid2field2value = filter_data(log, biospecimen_ssf_uuid2field2value, biospecimen_filters)
    
    clinical_metadata.update(clinical_omf_ssf_auxiliary_barcode2field2value)
    biospecimen_metadata.update(biospecimen_uuid2field2value)

def parse_archives(config, log, archives, study, archive2metadata, clinical_metadata, biospecimen_metadata, exclude_samples):
    '''
    downloads and unpacks the archives.  then parses, and if appropriate for the archive, uploads the files to GCS

    parameters:
        config: the configuration map
        log: logger to log any messages
        archives: information on the archives to unpack
        study: name of the TCGA study the files belongs to
        archive2metadata: metadata of the archive
        clinical_metadata: the return map for clinical metadata
        biospecimen_metadata: the return map for biospecimen metadata
    '''
    tmp_dir_parent = os.environ.get('ISB_TMP', '/tmp/')
    for archive_fields in archives:
        if not 'Level_1' in archive_fields[0]:
            log.info('skipping bio archive %s' % (archive_fields[0]))
            continue
        log.info('processing archive %s' % (archive_fields[0]))
        archive_path = os.path.join(tmp_dir_parent, archive_fields[0] + '/')
        if not os.path.isdir(archive_path):
            os.makedirs(archive_path)
        archive_path = util.setup_archive(config, archive_fields, log)
        files = os.listdir(archive_path)
        parse_files(config, log, files, archive_path, archive_fields, study, archive2metadata, exclude_samples, clinical_metadata, biospecimen_metadata)
        shutil.rmtree(archive_path)

def parse_bio(config, archives, study, archive2metadata, log_name):
    '''
    parses the files in the bio archive, and if appropriate, uploads them to GCS
    
    parameters:
        config: the configuration map
        archives: information on the archives to unpack
        study: name of the TCGA study the files belongs to
        archive2metadata: metadata of the archive
        log_name: the name of the log to use to log any messages
        
    return type:
        bio_metadata: when the Participant barcode is the key, the value is a map that captures values from the clinical file
          based on the fields in the config file under ['metadata_locations']['clinical'], when the Sample UUID is the 
          key, the value is a map that captures values from the biospeciman file based on the fields in the config file under 
          ['metadata_locations']['biospecimen']
        exclude_samples: set of samples where is_ffpe is YES or project element is missing
    '''
    log = logging.getLogger(log_name)
    log.info('start parse bio')
    study = study.lower()
    clinical_metadata = {}
    biospecimen_metadata = {}
    exclude_samples = set()

    parse_archives(config, log, archives, study, archive2metadata, clinical_metadata, biospecimen_metadata, exclude_samples)
    
    for barcode, term2value in biospecimen_metadata.iteritems():
        if 15 < len(barcode) and 'is_ffpe' in term2value and 'YES' == term2value['is_ffpe']:
            exclude_samples.add(barcode)
    
    # sanity check the samples against the participants
    participants = set()
    no_clinical = set()
    for uuid in biospecimen_metadata:
        barcode = biospecimen_metadata[uuid]['SampleBarcode']
        participants.add(barcode[:12])
        try:
            clinical_metadata[barcode[:12]]
        except:
            no_clinical.add(barcode[:12])
    
    if 0 < len(no_clinical):
        log.info('The following participants had no clinical data:\n\t%s' % ('\n\t'.join(no_clinical)))
    log.info('finished parse bio: %s participants, %s samples.' % (len(participants), len(biospecimen_metadata)))

    return clinical_metadata, biospecimen_metadata, exclude_samples


