#!/bin/bash

# Init Airflow DB si pas encore faite
airflow db check || airflow db init
exec airflow  "$@"
