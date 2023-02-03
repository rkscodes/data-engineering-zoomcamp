FROM prefecthq/prefect:2.7.7-python3.9

COPY docker-requirement.txt . 

RUN pip install -r docker-requirement.txt --trusted-host pypi.python.org --no-cache-dir

COPY week_2 /opt/prefect/flows  
COPY /week_2/02_gcp/data/yellow /opt/prefect/data/yellow 
