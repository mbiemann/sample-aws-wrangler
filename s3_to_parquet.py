import random
from datetime import datetime

import awswrangler as wr
import pandas as pd

###################################################################################################

def main(type=''):

    # Generate Random Record

    data = pd.DataFrame([{
        'event_date':     datetime.now().strftime('%Y-%m-%d'),
        'event_datetime': datetime.now().isoformat(),
        'event_name':     f'Event {chr(random.randint(65,90))}',
        'event_value':    random.randint(10000,20000),
        'event_col1':     random.randint(0,10),
        'event_col2':     random.randint(10,20)
    }])

    # Configurations

    bucket           = 'viadev-test-datalake-mb123'
    databaseName     = 'tempdb'
    tableName        = 'event'
    tableDescription = 'Random records for event template table'
    partitionCols    = ['event_date']
    datatypeCols     = {
        'event_date':    'string',
        'event_datetime':'string',
        'event_name':    'string',
        'event_value':   'bigint'
    }
    commentsCols     = {
        'event_date':    'Use this field in where to use partition and optimaze the select.',
        'event_datetime':'Date and time with ISO format',
        'event_name':    'The name random of event',
        'event_value':   'The value random of event'
    }
    # tableParameters  = {
    #     'last_updated':datetime.now().isoformat()
    # }
    ######## Gera um nova versão da tabela

    # Write to S3

    wr.s3.to_parquet(
        df=data,
        dataset=True,
        mode='append',
        # mode='overwrite',
        database=databaseName,
        table=tableName,
        description=tableDescription,
        # parameters=tableParameters,
        partition_cols=partitionCols,
        dtype=datatypeCols,
        columns_comments=commentsCols,
        path=f's3://{bucket}/{databaseName}/{tableName}/',
        compression='snappy'
        # catalog_versioning=True
    )

    # Repair Table
    ####### Não atualiza os campos

    # query_final_state = wr.athena.repair_table(
    #     database=databaseName,
    #     table=tableName
    # )
    # print(query_final_state)

###################################################################################################
### Local Execute
###################################################################################################

if __name__ == "__main__":

    main()

###################################################################################################