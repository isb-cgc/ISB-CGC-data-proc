'''
Created on Jan 12, 2015

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
import argparse

import sdrfClasses

platform2excludeNodes = {'mda_rppa_core': ['Source Name', 'Hybridization Name', 'Scan Name', 'Array Data File', 'Image File']}

# map of the main header node names to their class in ../util/sdrfClasses
name2node = {
    'Source Name': sdrfClasses.SourceName,
    'Sample Name': sdrfClasses.SampleName,
    'Extract Name': sdrfClasses.ExtractName,
    'Labeled Extract Name': sdrfClasses.LabeledExtractName,
    'Assay Name': sdrfClasses.AssayName,
    'Scan Name': sdrfClasses.ScanName,
    'Hybridization Name': sdrfClasses.HybridizationName,
    'Array Data File': sdrfClasses.ArrayDataFile,
    'Comment [Derived Data File REF]': sdrfClasses.DerivedDataFileREF,
    'Derived Array Data File': sdrfClasses.DerivedArrayDataFile,
    'Derived Data File REF': sdrfClasses.DerivedDataFileREF,
    'Derived Data File': sdrfClasses.DerivedDataFile,
    'Array Data Matrix File': sdrfClasses.ArrayDataMatrixFile,
    'Derived Array Data Matrix File': sdrfClasses.DerivedArrayDataMatrixFile,
    'Image File': sdrfClasses.ImageFile
}
# map of the edge node names to their class in ../util/sdrfClasses
name2edge = {
    'Protocol REF': sdrfClasses.ProtocolREF
}
# map of the qualified attribute node names to their class in ../util/sdrfClasses
name2termattr = {
    'Characteristics': sdrfClasses.Characteristics,
    'Factor Value': sdrfClasses.FactorValue,
    'Parameter Value': sdrfClasses.ParameterValue,
    'Unit': sdrfClasses.Unit,
    'Comment': sdrfClasses.Comment
}
# map of the plain attribute node names to their class in ../util/sdrfClasses
name2attr = {
    'Provider': sdrfClasses.Provider,
    'Material Type': sdrfClasses.MaterialType,
    'Technology Type': sdrfClasses.TechnologyType,
    'Label': sdrfClasses.Label,
    'Array Design File': sdrfClasses.ArrayDesignFile,
    'Array Design REF': sdrfClasses.ArrayDesignREF,
    'Array Name': sdrfClasses.ArrayName,
    'Annotation REF': sdrfClasses.AnnotationREF,
    'Annotations File': sdrfClasses.AnnotationsFile,
    'Data Transformation Name': sdrfClasses.DataTransformationName,
    'Normalization Name': sdrfClasses.NormalizationName,
    'Performer': sdrfClasses.Performer,
    'Date': sdrfClasses.Date,
    'Description': sdrfClasses.Description,
    'Term Source REF': sdrfClasses.TermSourceREF,
    'Term Accession Number': sdrfClasses.TermAccessionNumber,
}

class HeaderAttrinfo(object):
    '''
    this class holds the information on the attribute nodes.  the script will also add 
    the class needed to instantiated instances of this node, the name of the list or map
    instances will be placed on the parent node, and, for qualified headers, the qualifying
    term.  the name of the header is also added
    '''
    def __init__(self):
        self.index2attrs = []
        
class HeaderNodeinfo(object):
    '''
    this class holds the information on the major nodes.  the script will also add 
    the class needed to instantiated instances of this node and the name of the header
    '''
    def __init__(self):
        self.nodes = {}
        self.index2attrs = []
        self.index2inedges = []
        self.index2outedges = []
        
def findDataNodes(sdrf2index2nodeinfo, log):
    '''
    this is an exemplar for how to navigate through the header info and contained nodes
    '''
    log.info('checking for data nodes')
    for sdrf, index2nodeinfo in sdrf2index2nodeinfo.iteritems():
        log.info('%s:' % (sdrf))
        for headerinfo in index2nodeinfo:
            headerNode = headerinfo[1]
            level2count = {}
            level2examples = {}
            noLevels = False
            for node in headerNode.nodes.values():
                if not getattr(node, 'comment', None) or 0 == len(node.comment) or not node.comment.get('tcgadatalevel'):
                    log.info('\t\t%s is not a data node' % (headerNode.columnName))
                    noLevels = True
                    break
                level = node.comment['tcgadatalevel'].name
                count = level2count.setdefault(level, 0) + 1
                level2count[level] = count
                examples = level2examples.setdefault(level, [])
                examples += [node.name]
            if noLevels:
                continue
            if 1 == len(level2count):
                for level, count in level2count.iteritems():
                    log.info('\t\t%s has %s %s nodes like %s' % (headerNode.columnName, count, level, level2examples[level][0]))
            else:
                log.info('\t\t%s has:' % (headerNode.columnName))
                for level, count in level2count.iteritems():
                    log.info('\t\t\t%s %s nodes like %s' % (count, level, level2examples[level][0]))

    log.info('finished checking for data nodes')
        
def printAttrWarnings(index2attrs, log):
    '''
    prints any saved warnings from the parse of the SDRF
    '''
    for index2attr in index2attrs:
        count = 4
        for warning in index2attr[1].warnings:
            count -= 1
            if count < 0:
                break
            log.info(warning)
        
        printAttrWarningInfo(index2attr[1].index2attrs, log)

def printAttrWarningInfo(index2attrs, log):
    '''
    pass through to print warnings from attribute nodes
    '''
    printAttrWarnings(index2attrs, log)
    
def printProtocolWarningInfo(index2attrs, log):
    '''
    pass through to print warnings from edge nodes
    '''
    printAttrWarnings(index2attrs, log)
    
def printGraphInfo(index2nodeinfo, log):
    '''
    prints general information about the header nodes
    '''
    for index2node in index2nodeinfo:
        log.info('\t\t\t%s: %s' % (index2node[1].columnName, len(index2node[1].nodes)))
        printAttrWarningInfo(index2node[1].index2attrs, log)
        printProtocolWarningInfo(index2node[1].index2outedges, log)

def addAttrsToPath(attrs, line, index):
    '''
    adds this attribute into the path list and recurses into its attributes
    '''
    for attr in attrs:
        line[index] = attr.name
        index = index + 1
        index = addAttrsToPath(attr.attrs, line, index)
    return index

def addNodeToPath(nodes, line, index, verbose, count, recount, log):
    '''
    adds the major header node to the path and its attributes, and, if verbose and it's
    the last header node, prints out the line.  does a check to see if the lines being printed out 
    exceeds the number of lines from the original parse of the SDRF.  if so, will stop
    '''
    if recount[0] > count + 1000:
        return
    for node in nodes:
        line[index] = node.name
        nextIndex = index + 1
        nextIndex = addAttrsToPath(node.attrs, line, nextIndex)
        if 0 == len(node.nextNodes):
            if verbose:
                log.info('\t'.join(line))
            recount[0] += 1
        else:
            addNodeToPath(node.nextNodes, line, nextIndex, verbose, count, recount, log)

def gatherHeaderLine(index2nodes, line):
    '''
    puts together the header line into a list from the original SDRF
    '''
    for index2node in index2nodes:
        line += '%s\t' % (index2node[1].columnName)
        lastIndex = index2node[0]
        if len(index2node[1].index2attrs) > 0:
            line, lastIndex = gatherHeaderLine(index2node[1].index2attrs, line)
    return line, lastIndex
    
def printGraph(index2nodeinfo, verbose, count, log):
    '''
    reprints the graph if verbose is true.  counts the number of lines to compare
    to the original parse to see if the graph is consistent
    '''
    line, lastIndex = gatherHeaderLine(index2nodeinfo, '')
    if verbose:
        log.info(line)
    
    startNodes = index2nodeinfo[0][1].nodes
    line = ['' for _ in range(lastIndex + 1)]
    recount = [0]
    addNodeToPath(startNodes.values(), line, 0, verbose, count, recount, log)
    if recount[0] == count:
        log.info('\t\tpaths in graph consistent with lines in sdrf: %s == %s' % (recount[0], count))
    else:
        log.info('\t\tpaths in graph inconsistent with lines in sdrf: %s+ != %s' % (recount[0], count))
    
def printNestedAttr(index2nodes, indent, log):
    '''
    utility method to print out the parsed header line
    '''
    for index2node in index2nodes:
        log.info('%s%s(%s):' % (indent, index2node[1].columnName, index2node[0]))
        printNestedAttr(index2node[1].index2attrs, indent + '\t', log)
        if getattr(index2node[1], 'index2outedges', None):
            printNestedAttr(index2node[1].index2outedges, indent + '\t', log)

def processHeader(header, index2nodeinfo, excludeNodes, log):
    '''
    processes the header line into a list of major header inforamtion with each
    major header information having a list of its attributes and edges and reflection 
    information on those attributes and edges for the parse of each line
    '''
    fields = header.split('\t')
    curNodeList = []
    curInstanceList = []
    for index, field  in enumerate(fields):
        if field in name2node:
            headerNode = HeaderNodeinfo()
            headerNode.classIs = name2node[field]
            headerNode.columnName = field
            if field not in excludeNodes:
                index2nodeinfo += [[index, headerNode]]
            # always start lists over with new top-level header node
            curNodeList = [headerNode]
            curInstanceList = [headerNode.classIs('')]
        elif field in name2edge:
            headerAttrinfo = HeaderAttrinfo()
            headerAttrinfo.classIs = name2edge[field]
            headerAttrinfo.attr = 'outedges'
            instanceIs = headerAttrinfo.classIs('')
            headerAttrinfo.columnName = field
            headerAttrinfo.warnings = []
            # add as an out edge to the current node and save it on the list to be an in edge for the next node
            headerNode.index2outedges += [[index, headerAttrinfo]]

            curNodeList += [headerAttrinfo]
            curInstanceList += [instanceIs]
        else:
            headerAttrinfo = HeaderAttrinfo()
            if -1 < field.find('['):
                headerAttrinfo.classIs = name2termattr[field[:field.find('[')].strip()]
                instanceIs = headerAttrinfo.classIs('', '')
                headerAttrinfo.attr = field[:field.find('[')].strip().replace(' ', '').lower()
                headerAttrinfo.term =  field[field.find('[') + 1:field.find(']')].strip().replace(' ', '').lower()
                headerAttrinfo.isTermAttr = True
            elif field in name2attr:
                headerAttrinfo = HeaderAttrinfo()
                headerAttrinfo.classIs = name2attr[field]
                instanceIs = headerAttrinfo.classIs('')
                headerAttrinfo.attr = field.replace(' ', '').lower()
                headerAttrinfo.isTermAttr = False
            else:
                raise ValueError('%s is not recognized as a header' % (field))
            headerAttrinfo.columnName = field
            headerAttrinfo.warnings = []
            
            # the rule is that an attribute belongs to the nearest column to the left
            # for which it is allowed (from section 2.3 of MAGE-TAB 1.1)
            useNodeList = curNodeList[:]
            useNodeList.reverse()
            useInstanceList = curInstanceList[:]
            useInstanceList.reverse()
            for curNode, curInstance in zip(useNodeList, useInstanceList):
                if None != getattr(curInstance, headerAttrinfo.attr, None):
                    curNode.index2attrs += [[index, headerAttrinfo]]
                    break
                curNodeList = curNodeList[:-1]
                curInstanceList = curInstanceList[:-1]
            if 0 == len(curNodeList):
                raise ValueError('''didn't find a parent node for %s:%s''' % (headerAttrinfo.attr, field))
            curNodeList += [headerAttrinfo]
            curInstanceList += [instanceIs]
    # print back out the header information
    printNestedAttr(index2nodeinfo, '\t\t', log)
    
def verifyAttrs(fields, curHeaderNode, curNode, attriter):
    '''
    verifies that for a previously seen node, that its attributes have not taken on new values
    in the new line
    '''
    for attr in getattr(curHeaderNode, attriter, []):
        index = attr[0]
        headerAttrinfo = attr[1]
        value = fields[index]
        if getattr(headerAttrinfo, 'isTermAttr', None) and headerAttrinfo.isTermAttr:
            attrMap = getattr(curNode, headerAttrinfo.attr)
            instanceIs = attrMap[headerAttrinfo.term]
        else:
            attrList = getattr(curNode, headerAttrinfo.attr)
            instanceIs = attrList[0]
        if instanceIs.name != value:
            headerAttrinfo.warnings += ['\t\t\t===WARNING: values have changed for %s-%s(%s): %s != %s' % (curHeaderNode.columnName, headerAttrinfo.columnName, curNode.name, instanceIs.name, value)]
        verifyAttrs(fields, headerAttrinfo, instanceIs, attriter)

def addAttrs(fields, curHeaderNode, curNode, attriter):
    '''
    adds the attributes for a new node to the parent node
    '''
    for attr in getattr(curHeaderNode, attriter, []):
        index = attr[0]
        headerAttrinfo = attr[1]
        if getattr(headerAttrinfo, 'isTermAttr', None) and headerAttrinfo.isTermAttr:
            instanceIs = headerAttrinfo.classIs(fields[index], headerAttrinfo.term)
            attrMap = getattr(curNode, headerAttrinfo.attr)
            # currently don't expect this, but if it occurs, need to make the value on the map a list
            if headerAttrinfo.term in attrMap:
                raise ValueError('found more than one value for column %s' % (headerAttrinfo.columnName))
            attrMap[headerAttrinfo.term] = instanceIs
        else:
            attrList = getattr(curNode, headerAttrinfo.attr)
            for instanceIs in attrList:
                if instanceIs.name != fields[index]:
                    headerAttrinfo.warnings += ['\t\t\t===WARNING: additional value for %s-%s(%s): %s != %s' % 
                        (curHeaderNode.columnName, headerAttrinfo.columnName, curNode.name, instanceIs.name, fields[index])]
            instanceIs = headerAttrinfo.classIs(fields[index])
            attrList += [instanceIs]
        curNode.attrs += [instanceIs]
        addAttrs(fields, headerAttrinfo, instanceIs, attriter)

def processLine(line, index2nodeinfo, count, log):
    '''
    reads each line in the SDRF and instantiates the nodes based on the
    reflection information in the header node structure
    '''
    fields = line.split('\t')
    curNodes = []
    try:
        for index2node in index2nodeinfo:
            index = index2node[0]
            curHeaderNode = index2node[1]
            value = fields[index]
            if value in curHeaderNode.nodes:
                # we have seen this node, verify that its attributes
                # have the same values
                curNode = curHeaderNode.nodes[value]
                verifyAttrs(fields, curHeaderNode, curNode, 'index2attrs')
                verifyAttrs(fields, curHeaderNode, curNode, 'index2outedges')
            else:
                curNode = curHeaderNode.classIs(value)
                curHeaderNode.nodes[value] = curNode
                addAttrs(fields, curHeaderNode, curNode, 'index2attrs')
                addAttrs(fields, curHeaderNode, curNode, 'index2outedges')
            curNodes += [curNode]
        # now hook up this path
        for index, curNode in enumerate(curNodes):
            if not curNode.name or '->' == curNode.name:
                continue
            for offset in range(index-1, -1, -1):
                if curNodes[offset].name and '->' != curNodes[offset].name:
                    curNode.prevNodes.add(curNodes[offset])
                    break
            for offset in range(index+1, len(curNodes)):
                if curNodes[offset].name and '->' != curNodes[offset].name:
                    curNode.nextNodes[curNodes[offset]] = offset
                    break
    except Exception as e:
        log.exception('ERROR: problem occurred on line %s' % (count))
        raise e

def getExcludeNodes(sdrffile):
    '''
    reads in nodes that should be excluded from the final results.  this is useful when 
    the underlying graph isn't properly flattened
    '''
    for key in platform2excludeNodes:
        if key in sdrffile.lower():
            return platform2excludeNodes[key]
    return set()
    
def parseArgs(log):
    '''
    parse the arguments using the standard argparse module
    '''
    parser = argparse.ArgumentParser(description='input for the sdrf parse')
    parser.add_argument('sdrfs', nargs='+', help='files to parse')
    parser.add_argument('--verbose', '-v', action='store_true', help='whether to output the SDRF contents back to console')
    args = parser.parse_args()
    log.info(args)
    
    return args

def main(sdrfs, log, verbose = False):
    '''
    top-level method to parse the sdrf files
    '''
    log.info('\tstarting parseSDRF')
    sdrf2index2nodeinfo = {}
    for sdrffile in sdrfs:
        log.info('\t\tprocessing %s' % (sdrffile))
        try:
            with open(sdrffile) as sdrf:
                index2nodeinfo = []
                sdrf2index2nodeinfo[sdrffile] = index2nodeinfo
                excludeNodes = getExcludeNodes(sdrffile);
                header = sdrf.readline().strip()
                processHeader(header, index2nodeinfo, excludeNodes, log)
                count = 0
                for line in sdrf:
                    if count % 256 == 0:
                        log.info('\t\t\tprocessed %s lines' % (count))
                    count += 1
                    processLine(line.strip(), index2nodeinfo, count, log)
                log.info('\t\tprocessed %s total lines' % (count))
                printGraphInfo(index2nodeinfo, log)
                printGraph(index2nodeinfo, verbose, count, log)
                log.info('\t\tfinished %s' % (sdrffile))
        except:
            log.exception('==  ERROR: problem with parsing %s' % (sdrffile))
        
    log.info('\tfinished parseSDRF')
    return sdrf2index2nodeinfo
