USE DATABASE HOL_DB;
select * from public.my_first_analysis;

CREATE NOTIFICATION INTEGRATION SQLSCHOOL_SALES_PIPE
ENABLED=TRUE
TYPE=QUEUE
NOTIFICATION_PROVIDER=AZURE_STORAGE_QUEUE
AZURE_STORAGE_QUEUE_PRIMARY_URI='https://mvsnowflakestage.queue.core.windows.net/queue'
AZURE_TENANT_ID='*******';

SHOW INTEGRATIONS;

DESC NOTIFICATION INTEGRATION SQLSCHOOL_SALES_PIPE;

CREATE OR REPLACE FILE FORMAT SQLSCHOOLCSVFMT
TYPE= CSV FIELD_DELIMITER = ';' SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
NULL_IF = ('NULL', 'NULL')
EMPTY_FIELD_AS_NULL = TRUE;

CREATE OR REPLACE STAGE SQLSCHOOL_SALES_STAGE
URL='AZURE://mvsnowflakeinput.blob.core.windows.net/container/'
CREDENTIALS=(AZURE_SAS_TOKEN='*********')
FILE_FORMAT = SQLSCHOOLCSVFMT;

SHOW STAGES;

ls @SQLSCHOOL_SALES_STAGE;

CREATE OR REPLACE TABLE salesdata
(Nomes varchar,
Idades int, 
Peso float,
Altura float,
IMC float);

COPY INTO salesdata
from @SQLSCHOOL_SALES_STAGE
file_format = SQLSCHOOLCSVFMT
pattern='.*data,*.csv';

CREATE OR REPLACE PIPE "SQLSCHOOL_SNOW_PIPE"
auto_ingest = true
integration = 'SQLSCHOOL_SALES_PIPE'
as
copy into salesdata from @SQLSCHOOL_SALES_STAGE
file_format = SQLSCHOOLCSVFMT
pattern='.*data.*.csv';

select * from salesdata;

ALTER PIPE SQLSCHOOL_SNOW_PIPE REFRESH;

select * from salesdata;

SELECT *
FROM TABLE(information_schema.copy_history(table_name=>'salesdata', start_time=> dateadd(hours, -2,
current_timestamp())));

drop pipe SQLSCHOOL_SNOW_PIPE;
drop stage SQLSCHOOL_SALES_STAGE;