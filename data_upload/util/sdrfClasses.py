'''
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
class Node:
    def __init__(self, name):
        self.name = name
        self.attrs = []
        self.nextNodes = {}
        self.prevNodes = set()
        self.outedges = []

class SourceName(Node):
    cardinality = '1'
    def __init__(self, name):
        Node.__init__(self, name)
        self.characteristics = {}
        self.provider = []
        self.materialtype = []
        self.description = []
        self.comment = {}

class SampleName(Node):
    cardinality = '*'
    def __init__(self, name):
        Node.__init__(self, name)
        self.characteristics = {}
        self.materialtype = []
        self.description = []
        self.comment = {}

class ExtractName(Node):
    cardinality = '*'
    def __init__(self, name):
        Node.__init__(self, name)
        self.characteristics = {}
        self.materialtype = []
        self.description = []
        self.annotationref = []
        self.comment = {}

class LabeledExtractName(Node):
    cardinality = '1'
    def __init__(self, name):
        Node.__init__(self, name)
        self.characteristics = {}
        self.materialtype = []
        self.description = []
        self.label = []
        self.comment = {}

class AssayName(Node):
    cardinality = '1'
    def __init__(self, name):
        Node.__init__(self, name)
        self.technologytype = []
        self.arraydesignfile = []
        self.arraydesignref = []
        self.annotationref = []
        self.comment = {}

class ScanName(Node):
    cardinality = '*'
    def __init__(self, name):
        Node.__init__(self, name)
        self.comment = {}

class HybridizationName(Node):
    cardinality = '*'
    def __init__(self, name):
        Node.__init__(self, name)
        self.arraydesignref = []
        self.arraydesignfile = []
        self.comment = {}

class ArrayDataFile(Node):
    cardinality = '*'
    def __init__(self, name):
        Node.__init__(self, name)
        self.comment = {}

class DerivedArrayDataFile(Node):
    cardinality = '*'
    def __init__(self, name):
        Node.__init__(self, name)
        self.comment = {}
        
# this is non-conformant
class DerivedDataFileREF(Node):
    cardinality = '*'
    def __init__(self, name):
        Node.__init__(self, name)
        self.comment = {}
        
# this is non-conformant
class DerivedDataFile(Node):
    cardinality = '*'
    def __init__(self, name):
        Node.__init__(self, name)
        self.comment = {}
        # this is non-conformant
        self.parametervalue = {}

class ArrayDataMatrixFile(Node):
    cardinality = '*'
    def __init__(self, name):
        Node.__init__(self, name)
        self.comment = {}

class DerivedArrayDataMatrixFile(Node):
    cardinality = '*'
    def __init__(self, name):
        Node.__init__(self, name)
        self.comment = {}

class ImageFile(Node):
    cardinality = '*'
    def __init__(self, name):
        Node.__init__(self, name)
        self.comment = {}

class Attribute:
    def __init__(self, name):
        self.name = name
        self.attrs = []

class Characteristics(Attribute):
    cardinality = '*'
    def __init__(self, name, term):
        Attribute.__init__(self, name)
        self.term = term
        self.unit = {}
        self.term = []

class Provider(Attribute):
    cardinality = '1'
    def __init__(self, name):
        Attribute.__init__(self, name)
        self.comment = {}

class MaterialType(Attribute):
    cardinality = '1'
    def __init__(self, name):
        Attribute.__init__(self, name)
        self.termsourceref = []

class TechnologyType(Attribute):
    cardinality = '1'
    def __init__(self, name):
        Attribute.__init__(self, name)
        self.termsourceref = []

class Label(Attribute):
    cardinality = '1'
    def __init__(self, name):
        Attribute.__init__(self, name)
        self.termsourceref = []

class ArrayDesignFile(Attribute):
    cardinality = '1'
    def __init__(self, name):
        Attribute.__init__(self, name)
        self.termsourceref = []
        self.annotationsfile = []
        self.comment = {}

class ArrayDesignREF(Attribute):
    cardinality = '1'
    def __init__(self, name):
        Attribute.__init__(self, name)
        self.termsourceref = []
        self.comment = {}

class ArrayName(Attribute):
    cardinality = '1'
    def __init__(self, name):
        Attribute.__init__(self, name)

# this is non-conformant
class AnnotationREF(Attribute):
    cardinality = '1'
    def __init__(self, name):
        Attribute.__init__(self, name)

# this is non-conformant
class AnnotationsFile(Attribute):
    cardinality = '1'
    def __init__(self, name):
        Attribute.__init__(self, name)

class FactorValue(Attribute):
    cardinality = '*'
    def __init__(self, name, term):
        Attribute.__init__(self, name)
        self.term = term
        self.unit = {}
        self.term = []

# this is non-conformant, should be a node but rppa doesn't
# make it unique per data file so the graph isn't flattened
# properly
class NormalizationName(Attribute):
    cardinality = '*'
    def __init__(self, name):
        Attribute.__init__(self, name)
        self.comment = {}

# this is non-conformant
class DataTransformationName(Attribute):
    cardinality = '*'
    def __init__(self, name):
        Attribute.__init__(self, name)
        self.comment = {}

class Performer(Attribute):
    cardinality = '1'
    def __init__(self, name):
        Attribute.__init__(self, name)
        self.comment = {}

class Date(Attribute):
    cardinality = '1'
    def __init__(self, name):
        Attribute.__init__(self, name)

class ParameterValue(Attribute):
    cardinality = '*'
    def __init__(self, name, term):
        Attribute.__init__(self, name)
        self.term = term
        self.unit = {}
        self.comment = {}
        self.term = []

class Unit(Attribute):
    cardinality = '1'
    def __init__(self, name, term):
        Attribute.__init__(self, name)
        self.term = term
        self.termsourceref = []

class Description(Attribute):
    cardinality = '1'
    def __init__(self, name):
        Attribute.__init__(self, name)

class TermSourceREF(Attribute):
    cardinality = '1'
    def __init__(self, name):
        Attribute.__init__(self, name)
        self.termaccessionnumber = []

class TermAccessionNumber(Attribute):
    cardinality = '1'
    def __init__(self, name):
        Attribute.__init__(self, name)

class Comment(Attribute):
    cardinality = '*'
    def __init__(self, name, term):
        Attribute.__init__(self, name)
        self.term = term

class Edge(Attribute):
    def __init__(self, name):
        Attribute.__init__(self, name)

class ProtocolREF(Edge):
    cardinality = '*'
    def __init__(self, name):
        Edge.__init__(self, name)
        self.termsourceref = []
        self.parametervalue = {}
        self.performer = []
        self.date = []
        self.comment = {}
        # these is non-conpliant but relationship exists for protein arrays
        self.arrayname = []
        self.arraydesignfile = []
        self.datatransformationname = []
        self.normalizationname = []