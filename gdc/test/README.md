ISB-CGC-data-proc/gdc/test
================
these aren't unittests in the usual sense of running a known test
and using asserts to assure correct results.  they are currently
being used to drive parts of the code to fill in details without
overwriting other information in the metadata.  for instance, to
add image file information after the main metadata run or to fill
in the sample data availability table with the samples from a
BiqQuery table.
