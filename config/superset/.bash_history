exit
docker exec -it superset bash
python -c 'import psycopg2; conn = psycopg2.connect("host=postgres port=5432 dbname=postgres user=postgres password=postgres"); print("Connected!")'
python -c 'import psycopg2; conn = psycopg2.connect("host=postgres port=5432 dbname=postgres user=postgres password=postgres"); print("Connected!")'
exit
