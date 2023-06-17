Build and run:
  $ docker-compose build
  $ docker-compose up

From a separate terminal:
  Trigger the ETL process:
    $ curl localhost:8000/trigger_etl

  Query the postgres database for the derived values in the users table:
    $ curl localhost:8000/query_db

  When finished:
    $ docker-compose down

Using scripts:
  Build and run the docker container:
    $ bash setup_etl.sh
  From a separate terminal:
    Trigger the ETL process:
      $ bash trigger_etl.sh
    Query the database post-ETL:
      $ bash query_db.sh
    When finished:
      $ docker-compose down