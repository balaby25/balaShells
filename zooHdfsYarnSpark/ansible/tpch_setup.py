#!/scratch/balakcha/python/bin/python2.7

import time
import re
import os
import sys
import io
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

#DEPLOYMENT_DIR = config.get('environment', 'DEPLOYMENT_DIR')
DEPLOYMENT_DIR = "/scratch/balakcha"
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
ZOO_LOG_DIR = config.get('zoo', 'ZOO_LOG_DIR')
HDFS_DATA = config.get('hdfs', 'HDFS_DATA')
HDFS_LOG_DIR = config.get('hdfs', 'HDFS_LOG_DIR')
#print "read from config.cfg. default_java_hjome is : " +  DEFAULT_JAVA_HOME  + '\n'

def justPing():
    print "[INFO ] only pinging the hosts ."
    cmd="ansible -i hosts.ini myCluster  -m ping"
    os.system(cmd)

def installSBT():
    print "[INFO ] copying the needed source tars"
    cmd="cd " + DEPLOYMENT_DIR + " ; tar xvf  " + SOURCE_DIR + "/sbt-0.13.15.tgz "
    os.system(cmd)

def generateJarsFromScalaScripts():
     try:
        print "[INFO ] generating jars from scala scripts using sbt "
        cmd="cd  " + DEPLOYMENT_DIR + "/ansible; sh  sbt-generate-jars.sh hq1 "  + DEPLOYMENT_DIR
        #x=subprocess.check_output(os.system(cmd))
        subprocess.check_output(cmd, shell=True)
     except subprocess.CalledProcessError, e:
          print "hq1 jar generation failed  "  , e.output
          sys.exit()

     try:
        print "[INFO ] copying the needed source tars"
        cmd="cd  " + DEPLOYMENT_DIR + "/ansible; sh  sbt-generate-jars.sh hq5 "  + DEPLOYMENT_DIR
        #x=subprocess.check_output(os.system(cmd))
        subprocess.check_output(cmd, shell=True)
     except subprocess.CalledProcessError, e:
          print "hq5 jar generation failed  "  , e.output
          sys.exit()

def prepTpchYml(file_name, template_file, tpch_schema):
    print "[INFO ] generate prep-tpch.yml "
    # rapidpeta has path as data_8G. hence extracting schema_size to populate prep-tpch.yml from template
    schema_size = tpch_schema.rsplit('_',1)[1].upper()

    config = io.open(file_name, 'w')
    for line in io.open(template_file, 'r'):
        line = line.replace('$JAVA_HOME', JAVA_HOME )
        line = line.replace('HADOOP_HOME: $HADOOP_HOME', "HADDOP_HOME: " + HADOOP_HOME )
        line = line.replace('$HADOOP_CONF_DIR', HADOOP_CONF_DIR )
        line = line.replace('SPARK_HOME: $SPARK_HOME' ,  "SPARK_HOME: " + SPARK_HOME )
        line = line.replace('$SPARK_CONF_DIR', SPARK_CONF_DIR )
        line = line.replace('$LAUNCH_DIR', LAUNCH_DIR )
        line = line.replace('$TPCH_SCHEMA', tpch_schema )
        line = line.replace('$SCHEMASIZE', schema_size )
        config.write(line)
    config.close()

def genScalasFromTemplates(file_name, template_file, namenode_port, tpch_workload_size):
    print "[INFO ] generating  scalas from templates ."
    config = io.open(file_name, 'w')
    for line in io.open(template_file, 'r'):
        line = line.replace('$namenode_port', namenode_port )
        line = line.replace('$tpch_workload_size', tpch_workload_size)
        config.write(line)
    config.close()

def genSparkSubmit(qry_id,  tpch_schema):
    print "[INFO ] generating  spark submit from templates ."
    outfile="spark-submit-" + qry_id + ".py"
    templatefile="spark-submit.py.template" 
    spark_master_v = subprocess.check_output("awk 'c&&!--c;/sparkMaster/{c=1}' hosts.ini ", shell=True)
    spark_endpoint = spark_master_v.rstrip('\r\n') + ":6066"
    launch_dir_without_quotes=LAUNCH_DIR.replace('"','')
    config = io.open(outfile, 'w')
    for line in io.open(templatefile, 'r'):
        line = line.replace('$qry_id', qry_id )
        line = line.replace('$TPCH_SCHEMA', tpch_schema )
        line = line.replace('$LAUNCH_DIR', launch_dir_without_quotes )
        line = line.replace('$spark_endpoint', spark_endpoint )
        config.write(line)
    config.close()
    print ("run " + outfile )



if __name__ == "__main__":
    # by default do nothing
    op = "nothingToDo"
    ts=datetime.datetime.now().strftime('%Y-%m-%d_%H:%M')
    if len(sys.argv) == 3:
        op = sys.argv[1]
        tpch_schema_v = sys.argv[2]
        print "optiom is : " + op + "   schema is : " + tpch_schema_v
    else:
      print """[ERROR] Usage : ./tpch_setup.py <option> <tpch_schemaSize>  
for Example : ./tpch_setup.py  \"firstTimeTpch\"  \"tpch_8g\"  """
      sys.exit(1)


    if (op == "adhoc"):
        print "[INFO] ========== running adhoc comds   =========="
        genSparkSubmit("hq1",  tpch_schema_v)
        #awk 'c&&!--c;/BALA/{c=1}'  hosts.ini
    elif (op == "just_ping"):
        print "[INFO] ========== pinging the hosts   =========="
        justPing()
    elif (op == "gen_sparksubmit"):
        print "[INFO] ========== pinging the hosts   =========="
        genSparkSubmit("hq1", hdfsFS_v ,tpch_schema_v )
    elif (op == "firstTimeTpch"):
        print "[INFO] ========== first time TPCH    =========="
        prepTpchYml("prep-tpch.yml", "prep-tpch.yml.template", tpch_schema_v)

        namenode_v = subprocess.check_output("awk 'c&&!--c;/namenode/{c=1}' hosts.ini ", shell=True)
        hdfsFS_v = namenode_v.rstrip('\r\n') + ":9001"
        # print "hdfsFs is " + hdfsFS_v

        genScalasFromTemplates("parquet-gens.scala","parquet-gens.scala.template", hdfsFS_v , tpch_schema_v )
        genScalasFromTemplates("hq1.scala","hq1.scala.template", hdfsFS_v ,tpch_schema_v )
        genScalasFromTemplates("hq5.scala","hq5.scala.template", hdfsFS_v , tpch_schema_v )

        installSBT()
        generateJarsFromScalaScripts()

        genSparkSubmit("hq1",  tpch_schema_v)
        genSparkSubmit("hq5",  tpch_schema_v)

        cmd="ansible-playbook -i hosts.ini prep-tpch.yml"
        os.system(cmd)
    

        print """ [NEXT STEPS ] : please run  sparkSubmit.sh thingy
"""
    else:
        print "[ERROR] Unrecognized option (can be \"adhoc\", \"just_ping\" , \"firstTimeTpch\" , \"gen_sparksubmit\" ): "+op
