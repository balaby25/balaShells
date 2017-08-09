#!/scratch/balakcha/python/bin/python2.7

import time
import re
import os
import sys
import argparse
import ConfigParser
import threading
import datetime
import subprocess
import shutil
import xml.etree.ElementTree as ET



def justPing():
    print "[INFO ] only pinging the hosts ."
    cmd="ansible -i hosts.ini myCluster  -m ping"
    os.system(cmd)

def usageNotes():
    print """[USAGE ]   ./start-deployment.py <deployment_dir|help> 
see  README.md  at https://ol-bitbucket.us.oracle.com/users/balakrishnan.chandrasekaran_oracle.com/repos/balakcha/browse/zooHdfsYarnSpark 
"""
    sys.exit()

def createDeploymentDir():
    cmd="ansible -i hosts.ini myCluster -m shell -a \"mkdir -p +deployment_dir+ \""
    os.system(cmd)


def generateConfigCfg():
    print "[INFO ] generating config.cfg "
    config_cfg= "[environment] \n"
    config_cfg += "DEPLOYMENT_DIR = \"" + deployment_dir + "\"\n"
    config_cfg += "SOURCE_DIR = \"" + deployment_dir + "/source\""  + "\n"
    config_cfg += "CLUSTER_CONF_DIR = \"" + deployment_dir + "/cluster_confs\""  + "\n"
    config_cfg += "CLUSTER_LOGS_DIR = \"" + deployment_dir + "/cluster_logs\""  + "\n"
    config_cfg += "LAUNCH_DIR = \"" + deployment_dir + "/launchdir\""  + "\n"
    config_cfg += "DEFAULT_JAVA_HOME = \"" + deployment_dir + "/dummyJavaHome\""  + "\n"
    config_cfg += "JAVA_HOME = \"" + deployment_dir + "/java\""  + "\n"
    config_cfg += "ZOOKEEPER_HOME = \"" + deployment_dir + "/zookeeper\""  + "\n"
    config_cfg += "HADOOP_HOME = \"" + deployment_dir + "/hadoop\""  + "\n"
    config_cfg += "SPARK_HOME = \"" + deployment_dir + "/spark\""  + "\n"
    config_cfg += "ZOOKEEPER_CONF_DIR = \"" + deployment_dir + "/cluster_confs/zookeeper\""  + "\n"
    config_cfg += "HADOOP_CONF_DIR = \"" + deployment_dir + "/cluster_confs/hadoop\""  + "\n"
    config_cfg += "SPARK_CONF_DIR = \"" + deployment_dir + "/cluster_confs/spark\""  + "\n"
    config_cfg += "\n"
    config_cfg += "[zoo] \n"
    config_cfg += "ZOO_DATA_DIR = " + deployment_dir + "/cluster_confs/zookeeper/dataDir"  + "\n"
    config_cfg += "ZOO_LOG_DIR = \"" + deployment_dir + "/cluster_logs/zookeeper\""  + "\n"
    config_cfg += "\n"
    config_cfg += "[hdfs] \n"
    config_cfg += "HDFS_DATA = " + deployment_dir + "/hadoop_data"  + "\n"
    config_cfg += "HDFS_LOG_DIR = " + deployment_dir + "/cluster_logs/hadoop"  + "\n"
    config_cfg += "[spark] \n"
    config_cfg += "SPARK_LOG_DIR = \"" + deployment_dir + "/cluster_logs/spark\""  + "\n"

    with open("config.cfg", "w") as text_file:
         text_file.write("{}".format(config_cfg))


def adhoc():
    print "[INFO ]  touch ing a file "
    #cmd="ansible -i hosts.ini myCluster  -m shell -a \"touch /tmp/balaTouch  \""
    #os.system(cmd)

    config = ConfigParser.RawConfigParser()
    config = ConfigParser.ConfigParser()
    configPath = os.getcwd()
    config.read(configPath + '/config.cfg')

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
    print "read from config.cfg. default_java_hjome is : " +  DEFAULT_JAVA_HOME  + '\n'
    ZOO_DATA_DIR = config.get('zoo', 'ZOO_DATA_DIR')
    HDFS_DATA = config.get('hdfs', 'HDFS_DATA')



if __name__ == "__main__":
    # by default do nothing
    op = "nothingToDo"
    if len(sys.argv) == 2:
        op = sys.argv[1]
        deployment_dir = sys.argv[1]
        if deployment_dir == "help":
            usageNotes()
            sys.exit(0)
      
        #createDeploymentDir()
        generateConfigCfg()
        print "[ACTION] run load_setup.py new_set_of_hosts"

    else:
       print ( "USAGE ./start-deployment.py <deployment_dir> " )
       sys.exit(1)
