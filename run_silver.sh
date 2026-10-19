docker rm -f silver_job 2>/dev/null
MSYS_NO_PATHCONV=1 docker run --name silver_job -u root --memory=3g -v "E:/DE_Projects/nyc-taxi-data-lake:/work:ro" apache/spark:3.5.9-python3 /opt/spark/bin/spark-submit --master "local[1]" --driver-memory 2g /work/silver_clean.py
