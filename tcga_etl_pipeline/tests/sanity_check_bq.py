import argparse
import httplib2
import os
import pandas
import sys
import time

from apiclient.discovery import build

from oauth2client.client import flow_from_clientsecrets
from oauth2client.file   import Storage
from oauth2client        import tools
from convert_gbq_to_dataframe import convert_gbq_to_dataframe
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials


#------------------------------------------------------------------------------

# Google project id, where to find client secrets, etc
PROJECT_ID = 'isb-cgc'
#CLIENTSECRETS_LOCATION = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
CLIENTSECRETS_LOCATION = 'client_secrets.json'
FLOW = flow_from_clientsecrets ( CLIENTSECRETS_LOCATION,
           scope='https://www.googleapis.com/auth/bigquery' )

#------------------------------------------------------------------------------
## bqTable is something like "isb-cgc:tcga_data_open.Biospecimen"

def parseBQtableName ( bqTable ):

    i1 = bqTable.find(":")
    print i1

    if ( i1 > 0 ):
        i2 = bqTable.find(".", i1 )
        print i2
        projectId = bqTable[:i1]
        datasetId = bqTable[i1+1:i2]
        tableId = bqTable[i2+1:]
    else:
        i2 = bqTable.find(".")
        print i2
        if ( i2 < 2 ):
            print " ERROR ??? table name must include dataset name !!! "
            print bqTable
            sys.exit(-1)
        else:
            projectId = PROJECT_ID
            datasetId = bqTable[:i2]
            tableId = bqTable[i2+1:]

    return ( projectId, datasetId, tableId )

#------------------------------------------------------------------------------

def runQuery ( queryStr ):
    print queryStr
    return (convert_gbq_to_dataframe( project_id=PROJECT_ID, query=queryStr))
#    return ( pandas.io.gbq.read_gbq ( query=queryStr, project_id=PROJECT_ID ) )

#------------------------------------------------------------------------------

def getSchemaInfo ( bigquery_service, bqTable ):

    ( projectId, datasetId, tableId ) = parseBQtableName ( bqTable )

    try:
        tables = bigquery_service.tables()
        table_info = tables.get ( projectId=projectId, datasetId=datasetId, tableId=tableId ).execute()
    except:
        print " ERROR: failed in tables.get() call to bigquery tables service "
        return ( -1, [], -1 )

    fieldNames = []
    for aField in table_info['schema']['fields']:
        try:
            fieldMode = aField['mode']
        except:
            fieldMode = "NA"
        if ( fieldMode == "REPEATED" ):
            for bField in aField['fields']:
                fieldNames += [ aField['name'] + "." + bField['name'] ]
                print fieldNames[-1], len(fieldNames)
        else:
            fieldNames += [ aField['name'] ]
            print fieldNames[-1], len(fieldNames)

    numFields = len(fieldNames)
    print numFields

    numRows = int ( table_info['numRows'] )

    return( numFields, fieldNames, numRows )

#------------------------------------------------------------------------------

def getSchemaInfo_old ( bqTable ):

    ## initial query is really just a test and to figure out what the
    ## schema looks like ...
    try:
        queryStr = "SELECT * FROM [%s] LIMIT 1" % bqTable
        print " queryStr : <%s> " % queryStr
        df = runQuery ( queryStr )

        ## print " writing out df "
        ## print df
        ## print df.columns

        print " "
        for aCol in df.columns:
            print "    ", aCol
        print " "
        numFields = len(df.columns)
        fieldNames = df.columns

    except:
        print " FAILED to get schema using a simple query "
        numFields = -1
        fieldNames = []

    return ( numFields, fieldNames )

#------------------------------------------------------------------------------

def getNumRows_old ( bqTable, fieldNames ):

    try:
        ## now let's just figure out how many rows we have total
        aCol = fieldNames[0]
        queryStr = "SELECT "
        queryStr  = 'SELECT {0} AS f '.format(aCol)
        queryStr += 'FROM [{0}] '.format(bqTable)
        r0 = runQuery ( queryStr )
        numRows = len(r0.values)
    except:
        print " ERROR ... query to find total number of rows FAILED ... ", aCol
        numRows = 999999999

        if ( "ParticipantBarcode" in fieldNames ):
            aCol = "ParticipantBarcode"

            queryStr  = '  SELECT {0}, COUNT({0}) AS unique_num '.format(aCol)
            queryStr += '  FROM [{0}] '.format(bqTable)
            queryStr += '  WHERE {0} IS NOT NULL '.format(aCol)
            queryStr += '  GROUP BY {0} ORDER BY unique_num DESC '.format(aCol)
            r0 = runQuery ( queryStr )
            numRows = 0
            for ii in range(len(r0.values)):
                numRows += r0.values[ii][1]
            print "     --> got numRows = %d " % numRows

    return ( numRows )

#------------------------------------------------------------------------------

def getNumNullValues ( aCol, bqTable ):

    try:
        queryStr = "SELECT {0} AS f, COUNT(*) AS n FROM [{1}] WHERE {0} IS NULL GROUP BY f".format(aCol, bqTable)
        r1 = runQuery ( queryStr )
        numNull = r1.n[0]
    except:
        numNull = 0

    return ( numNull )

#------------------------------------------------------------------------------

def getNumUniqueValues ( aCol, bqTable, numRows ):

    try:
        queryStr = "SELECT COUNT ( DISTINCT {0}, {1} ) AS n FROM [{2}] WHERE {0} IS NOT NULL".format(aCol, min(numRows+100,50000), bqTable)
        r2 = runQuery ( queryStr )
        numUnique = r2.n[0]
    except:
        print " ERROR counting up number of unique non-null values ", aCol
        numUnique = -1

    return ( numUnique )

#------------------------------------------------------------------------------

def get_MinMaxAvgCounts_by_Value ( aCol, bqTable ):

    queryStr  = 'SELECT MIN(unique_num) AS min_count, MAX(unique_num) AS max_count, AVG(unique_num) AS avg_count '
    queryStr += 'FROM ( '
    queryStr += '  SELECT {0}, COUNT({0}) AS unique_num '.format(aCol)
    queryStr += '  FROM [{0}] '.format(bqTable)
    queryStr += '  WHERE {0} IS NOT NULL '.format(aCol)
    queryStr += '  GROUP BY {0} ORDER BY unique_num DESC '.format(aCol)
    queryStr += ') '
    r3 = runQuery ( queryStr )

    return ( r3.min_count[0], r3.max_count[0], r3.avg_count[0] )

#------------------------------------------------------------------------------

def getNumUniqueValues_byPct ( aCol, bqTable, pct ):
    
    queryStr  = 'SELECT {0} AS f, COUNT(*) AS n '.format(aCol)
    queryStr += 'FROM [{0}] '.format(bqTable)
    queryStr += 'WHERE {0} IS NOT NULL '.format(aCol)
    queryStr += 'GROUP BY f '
    queryStr += 'ORDER BY n DESC '      ## no LIMIT because we might need all of them
    r4 = runQuery ( queryStr )
    gotN = len(r4.values)
    
    numTot = 0
    for ii in range(gotN):
        numTot += r4.values[ii][1]
    
    nump = float(numTot) * pct
    numUp = 0
    numTot = 0
    for ii in range(gotN):
        if ( numTot < nump ): numUp += 1
        numTot += r4.values[ii][1]

    if ( r4.values[gotN-1][1] < r4.values[0][1] ):
        for ii in range(min(gotN,5)):
            print "        %s  %d " % ( r4.values[ii][0], r4.values[ii][1] )

    if ( gotN > 0 ):
        mostFreqValue = r4.values[0][0]
        mostFreqCount = r4.values[0][1]
    else:
        mostFreqValue = ''
        mostFreqCount = ''
    
    return ( numUp, mostFreqValue, mostFreqCount )
    
#------------------------------------------------------------------------------

def main():

    ## - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
    ## initial set up ...
    parser = argparse.ArgumentParser ( parents=[tools.argparser] )
    parser.add_argument ( '--bqTable', '-t', type=str, required=True, action='store' )
    flags = parser.parse_args()

    ## check that the input table name looks ok
    ## example bqTable = "isb-cgc:tcga_data_open.Biospecimen"
    bqTable = flags.bqTable
    if ( len(bqTable) < 2 ): return
    print "     ", bqTable
    ( projectId, datasetId, tableId ) = parseBQtableName ( bqTable )
    print "         ", projectId, datasetId, tableId
    print " "

    #@!storage = Storage('bigquery_credentials.dat')
    #@!credentials = storage.get()

    #@!if credentials is None or credentials.invalid:
    #@!  credentials = tools.run_flow ( FLOW, storage, flags )

    #@!http = httplib2.Http()
    #@!http = credentials.authorize(http)

    #@!bigquery_service = build('bigquery', 'v2', http=http)

    credentials = GoogleCredentials.get_application_default()

    # Construct the service object for interacting with the BigQuery API.
    bigquery_service = discovery.build('bigquery', 'v2', credentials=credentials)

    ## - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

    print " " 
    print " "
    print " running sanity_check_bq on %s (%s) ... " % ( bqTable, tableId )
    print " "

    ## NOTE: The original implementation used the functions getSchemaInfo_old
    ## and getNumRows_old to find the names of the fields and the number of
    ## rows.  These functions are still in this file for reference, but the
    ## functionality has been re-implemented.
    
    if ( 0 ):
        ( numFields, fieldNames ) = getSchemaInfo_old ( bqTable )
        numRows = getNumRows_old ( bqTable, fieldNames )
    else:
        ( numFields, fieldNames, numRows ) = getSchemaInfo ( bigquery_service, bqTable )

    print " numFields = ", numFields
    print " numRows   = ", numRows
    print " "

    ## open output report file
    fh = file ( "%s.report.tsv" % tableId, 'w' )
    ## create and write out header row
    outLine = "field\tnum_null\tnum_unique\tmin\tmax\tavg\tnum_u90\tvalue\tcount\t"
    fh.write ( "%s\n" % outLine )

    ## now we loop over the columns
    for aCol in fieldNames:

        print "         now examining <%s> field " % aCol

        ## start the output row with the column name
        outLine = "%s\t" % aCol

        ## first we want to find out how many NULL records there are...
        numNull = getNumNullValues ( aCol, bqTable )
        outLine += "%d\t" % numNull

        ## next we want to find out the number of unique non-null values
        numUnique = getNumUniqueValues ( aCol, bqTable, numRows )
        outLine += "%d\t" % numUnique

        ## looking at the numRows, numNull, and numUnique values and write out
        ## some warnings as needed ...
        if ( (numRows-numNull) == 0 ):
            print " WARNING !!! all values are NULL !!! ", aCol
            print "\n %s : numRows = %d    numNull = %d    numUnique = %d (0) (0) " % \
                ( aCol, numRows, numNull, numUnique )
        else:
            if ( numUnique > 0 ):
                print "\n %s : numRows = %d    numNull = %d    numUnique = %d (%d) (%.1f) " % \
                    ( aCol, numRows, numNull, numUnique, numRows-numNull, (float(numRows-numNull)/float(numUnique)) )
                if ( (numRows-numNull) == numUnique ): print " EVERY value is UNIQUE ", aCol
            elif ( numUnique == 0 ):
                print " WARNING !!! ZERO unique values ??? ", aCol
                print "\n %s : numRows = %d    numNull = %d    numUnique = %d (%d) (inf) " % \
                    ( aCol, numRows, numNull, numUnique, numRows-numNull )

        ## if we don't have at least one non-null value, we can skip the rest ...
        if ( numUnique < 1 ): 
            outLine += "\t\t\t\t\t\t"

        ## and if every row is unique, we can also skip
        elif ( numUnique == (numRows-numNull) ):
            outLine += "\t\t\t\t\t\t"
        
        else:

            ## now we want to find out how frequently these unique values occur
            ( minCount, maxCount, avgCount ) = get_MinMaxAvgCounts_by_Value ( aCol, bqTable )
            outLine += "%d\t%d\t%.1f\t" % ( minCount, maxCount, avgCount )

            ## if there are too many unique values, then it's probably not very useful
            ## to dig much further into this ...
            if ( numUnique > 5000 ):
                outLine += "\t\t\t"

            else:
    
                ## now we want to find out how many different values we need
                ## to capture 90% of the rows...
                ## for example:
                ##          field name      ParticipantBarcode
                ##          num_unique      11365
                ##          num_90          10121
                ##          total_rows      23797

                pct90 = 0.90
                ( numU90, mostFreqValue, mostFreqCount ) = getNumUniqueValues_byPct ( aCol, bqTable, pct90 )

                outLine +="%d\t" % numU90
    
                if ( mostFreqValue != '' ):
                    outLine += "%s\t%d\t" % ( mostFreqValue, mostFreqCount )
                else:
                    outLine += "\t\t"
    
        fh.write ( "%s\n" % outLine )

    fh.close()

#------------------------------------------------------------------------------

if __name__ == '__main__':


    t0 = time.time() 
    main()
    t1 = time.time()

    print " "
    print " =============================================================== "
    print " "
    print " time taken in seconds : ", (t1-t0)
    print " "
    print " =============================================================== "

#------------------------------------------------------------------------------
