import requests
import pandas as pd
import json
import numpy as np

def get_hgnc_map():
    #------------------------------------------
    # Get HGNC approved, alias, previous symbols
    #------------------------------------------

    HGNC_url = "http://rest.genenames.org/fetch/status/Approved"
    headers = {'Accept': 'application/json'}
    resp = requests.get(HGNC_url, headers=headers)

    data = json.loads(resp.content)

    row_list = []
    for hgnc in data["response"]['docs']:
        
        # alias symbol
        alias_symbol = hgnc.get('alias_symbol')
        if alias_symbol:
            alias_symbol = ";".join(alias_symbol)
       
        # prev symbol
        prev_symbol = hgnc.get('prev_symbol')
        if prev_symbol:
            prev_symbol = ";".join(prev_symbol)

       # store symbol, alias, previous symbol
        row_list.append(
            {
              'entrez_id' : hgnc.get('entrez_id', np.nan)
             ,'symbol': hgnc.get('symbol', np.nan)
             ,'alias_symbol': alias_symbol
             ,'prev_symbol': prev_symbol
            }
        )
    # create a dataframe with HGNC information; use this to query information about HGNC genes
    hgnc_df = pd.DataFrame(row_list)
 
    return hgnc_df
