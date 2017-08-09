#!/bin/bash
cd /scratch/balakcha/ansible
./start-deployment.py "/scratch/balakcha"
./load_setup.py "new_set_of_hosts"
./cluster-admin.py "new_set_of_hosts"
./tpch_setup.py "firstTimeTpch" "tpch_8g"
