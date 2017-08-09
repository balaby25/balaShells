#!/scratch/balakcha/python/bin/python2.7

import time
import re
import os
import sys
import ConfigParser
import threading
import datetime
import subprocess
import shutil
import xml.etree.ElementTree as ET


config = ConfigParser.RawConfigParser()
config = ConfigParser.ConfigParser()
configPath = os.getcwd()
config.read(configPath + '/config.cfg')

DEPLOYMENT_DIR = config.get('environment', 'DEPLOYMENT_DIR')
SOURCE_DIR = config.get('environment', 'SOURCE_DIR')
CLUSTER_CONF_DIR = config.get('environment', 'CLUSTER_CONF_DIR')
CLUSTER_LOGS_DIR = config.get('environment', 'CLUSTER_LOGS_DIR')
LAUNCH_DIR = config.get('environment', 'LAUNCH_DIR')
DEFAULT_JAVA_HOME = config.get('environment', 'DEFAULT_JAVA_HOME')
JAVA_HOME = config.get('environment', 'JAVA_HOME')
ZOOKEEPER_HOME = config.get('environment', 'ZOOKEEPER_HOME')
HADOOP_HOME = config.get('environment', 'HADOOP_HOME')
SPARK_HOME = config.get('environment', 'SPARK_HOME')
ZOOKEEPER_CONF_DIR = config.get('environment', 'ZOOKEEPER_CONF_DIR')
HADOOP_CONF_DIR = config.get('environment', 'HADOOP_CONF_DIR')
SPARK_CONF_DIR = config.get('environment', 'SPARK_CONF_DIR')
ZOO_DATA_DIR = config.get('zoo', 'ZOO_DATA_DIR')
ZOO_LOG_DIR = config.get('zoo', 'ZOO_DATA_DIR')
HDFS_DATA = config.get('hdfs', 'HDFS_DATA')
SPARK_LOG_DIR = config.get('spark', 'SPARK_LOG_DIR')
#print "read from config.cfg. default_java_hjome is : " +  DEFAULT_JAVA_HOME  + '\n'


def justPing():
    print "[INFO ] only pinging the hosts ."
    cmd="ansible -i hosts.ini myCluster  -m ping"
    os.system(cmd)

def hdfsFormatOneTime():
    print "[INFO ] formatting hdfs "
    cmd="ansible -i hosts.ini namenode  -m shell -a \"export JAVA_HOME="+JAVA_HOME+" ; cd "+HADOOP_HOME+"; yes Y | ./bin/hdfs --config "+HADOOP_CONF_DIR+" namenode -format \""
    os.system(cmd)


def zooStart():
    print "[INFO ] starting zoo "
    cmd="ansible-playbook -i hosts.ini zoo-start.yml"
    os.system(cmd)

def hdfsStart():
    print "[INFO ] starting dfs.sh hdfs "
    cmd="ansible-playbook -i hosts.ini hdfs-start.yml"
    os.system(cmd)

def sparkStart():
    print "[INFO ] starting spark "
    cmd="ansible-playbook -i hosts.ini spark-start.yml"
    os.system(cmd)

def zooStop():
    print "[INFO ] stopping zookeeper "
    cmd="ansible-playbook -i hosts.ini zoo-stop.yml"
    os.system(cmd)

def hdfsStop():
    print "[INFO ] stopping   hdfs "
    cmd="ansible-playbook -i hosts.ini hdfs-stop.yml"
    os.system(cmd)

def sparkStop():
    print "[INFO ] stopping spark "
    cmd="ansible-playbook -i hosts.ini spark-stop.yml"
    os.system(cmd)

def checkZoo():
    print "[INFO ] checking zoo ."

    found = False
    zoo_cnt = 0
    f = open("hosts.ini", "r")
    lines = f.read().splitlines()
    for line in lines:
      if '[zookeeper]' in line :
            found = True
            continue
      if not line.strip() :
         found = False
         continue

      if found:
           zoo_cnt += 1
           # print line
           #zoo_cfg += "server." + str(zoo_cnt) + "=" + line + ":2888:3888\n"
           print line
           cmd="echo ruok | nc " + line + " 2181"
           os.system(cmd)
           print "\n"



if __name__ == "__main__":
    # by default do nothing
    op = "nothingToDo"
    if len(sys.argv) == 2:
        op = sys.argv[1]

    ts=datetime.datetime.now().strftime('%Y-%m-%d_%H:%M')

    if (op == "adhoc"):
        print "[INFO] ========== running adhoc comds   =========="
        hdfsStart()
    elif (op == "just_ping"):
        print "[INFO] ========== pinging the hosts   =========="
        justPing()
    elif (op == "start-zoo"):
        zooStart()
    elif (op == "start-hdfs"):
        hdfsStart()
    elif (op == "start-spark"):
        sparkStart()
    elif (op == "start-all"):
        zooStart()
        hdfsStart()
        sparkStart()
    elif (op == "stop-spark"):
        sparkStop()
    elif (op == "stop-hdfs"):
        hdfsStop()
    elif (op == "stop-zoo"):
        zooStop()
    elif (op == "stop-all"):
        sparkStop()
        hdfsStop()
        zooStop()
    elif (op == "new_set_of_hosts"):
         cmd="ansible-playbook -i hosts.ini zoo-setup.yml"
         os.system(cmd)
         zooStart()
         checkZoo()
         cmd="ansible-playbook -i hosts.ini hdfs-setup.yml"
         os.system(cmd)
         hdfsFormatOneTime()
         hdfsStart()
         cmd="ansible-playbook -i hosts.ini spark-setup.yml"
         os.system(cmd)
         sparkStart()
         print "[ACTION TIME] next is to run ./tpch-setup.py \"firstTimeTpch\" "
    elif (op == "check-zoo"):
        checkZoo()
    else:
        print "[ERROR] Unrecognized option (can be \"adhoc\", \"just_ping\" , \"start-all\" , \"start-zoo\" , \"start-hdfs\", \"start-spark\", \"stop-spark\", \"stop-hdfs\", \"stop-zoo\",  \"stop-all\" , \"check-zoo\", \"hdfs-format-one-time\" \"new_set_of_hosts\" ): "+op


