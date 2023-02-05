FROM prefecthq/prefect:2.7.7-python3.9

COPY docker-requirement.txt . 

RUN pip install -r docker-requirement.txt --trusted-host pypi.python.org --no-cache-dir

COPY week_2/03_deployment/ /opt/prefect/flows  
RUN mkdir -p data/yellow
