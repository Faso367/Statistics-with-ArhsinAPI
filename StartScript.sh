#!/bin/bash

# Параметры подключения к базе данных
DB_NAME="Arshindb"
DB_USER="postgres"
DB_PASSWORD="password"
DB_HOST="localhost"  # или адрес вашего сервера

# PG_USER="postgres"
# PG_HOST="localhost"
# PG_PORT="5432"

#psql -U $PG_USER -h $PG_HOST -p $PG_PORT -c "CREATE DATABASE $DB_NAME;"
#psql -U $DB_HOST -U $DB_USER -d $DB_NAME -f CreateScript.sql
python3 arshinDataDownloader.py
# psql -U $DB_HOST -U $DB_USER -d $DB_NAME -f ClearScript.sql
# psql -U $DB_HOST -U $DB_USER -d $DB_NAME -f CreateScript2.sql
#python3 addResultValidData.py
#python3 arshinDataDownloader.py