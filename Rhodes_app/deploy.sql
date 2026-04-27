USE DATABASE db_Real_Estate;
USE SCHEMA raw;

CREATE OR REPLACE STAGE streamlit_stage;

-- run this in SnowSQL only
PUT file://C:\projects\VsCodeWorkSpace\streamlit-for-snowflake\Rhodes_app\app.py @streamlit_stage
    OVERWRITE=TRUE
    AUTO_COMPRESS=FALSE;

CREATE OR REPLACE STREAMLIT Homebuilder_Sales
    ROOT_LOCATION = '@raw.streamlit_stage'
    MAIN_FILE = 'app.py'
    QUERY_WAREHOUSE = 'dw_Real_Estate';

SHOW STREAMLITS;