To build and run:
  docker-compose build
  docker-compose up

To trigger the ETL process (from a separate terminal):
  curl localhost:8000/trigger_etl

Using scripts:
  To build and run the docker container:
    bash setup_etl.sh
  To trigger the ETL process (from a separate terminal):
    bash trigger_etl.sh
  To query the database post-ETL (from a separate terminal):
    bash query_db.sh