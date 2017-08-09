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

#DEFAULT_JAVA_HOME = "/scratch/balakcha/dummy"
#JAVA_HOME = "/scratch/balakcha/jdk1.8.0_131"
#ZOOKEEPER_HOME = "/scratch/balakcha/zookeeper"
#HADOOP_HOME = "/scratch/balakcha/hadoop"
#SPARK_HOME = "/scratch/balakcha/spark"
#CLUSTER_CONF_DIR = "/scratch/balakcha/cluster_confs/"
#ZOOKEEPER_CONF_DIR = "/scratch/balakcha/cluster_confs/zookeeper"
#HADOOP_CONF_DIR = "/scratch/balakcha/cluster_confs/hadoop"
#SPARK_CONF_DIR = "/scratch/balakcha/cluster_confs/spark"

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
ZOO_LOG_DIR = config.get('zoo', 'ZOO_LOG_DIR')
HDFS_DATA = config.get('hdfs', 'HDFS_DATA')
HDFS_LOG_DIR = config.get('hdfs', 'HDFS_LOG_DIR')
SPARK_LOG_DIR = config.get('spark', 'SPARK_LOG_DIR')
#print "read from config.cfg. default_java_hjome is : " +  DEFAULT_JAVA_HOME  + '\n'


def justPing():
    print "[INFO ] only pinging the hosts ."
    cmd="ansible -i hosts.ini myCluster  -m ping"
    os.system(cmd)

def dummytest():
    print "[INFO ] dummy tests"
    cmd="ansible -i hosts.ini BALA  -m shell -a \"mkdir " +  LAUNCH_DIR  +  "\""
    os.system(cmd)

def copySourceTars():
    print "[INFO ] copying the needed source tars"
    cmd="ansible -i hosts.ini myCluster  -m shell -a \"mkdir " + SOURCE_DIR + "\""
    os.system(cmd)
    cmd="ansible -i hosts.ini myCluster  -m copy -a \"src=./cluster-env.sh dest=" + DEPLOYMENT_DIR  + "\""
    os.system(cmd)
    print "[INFO ] copying over the files ."
    cmd="ansible -i hosts.ini myCluster  -m copy -a \"src=../source/zookeeper-3.4.10.tar.gz dest=" + SOURCE_DIR + "\""
    os.system(cmd)
    cmd="ansible -i hosts.ini myCluster  -m copy -a \"src=../source/hadoop-2.8.0.tar.gz dest=" + SOURCE_DIR + "\""
    os.system(cmd)
    cmd="ansible -i hosts.ini myCluster  -m copy -a \"src=../source/spark-2.1.1-bin-hadoop2.7.tgz dest=" + SOURCE_DIR + "\""
    os.system(cmd)
    cmd="ansible -i hosts.ini myCluster  -m copy -a \"src=../source/jdk-8u131-linux-x64.tar.gz  dest=" + SOURCE_DIR + "\""
    os.system(cmd)
    cmd="ansible -i hosts.ini myCluster  -m copy -a \"src=../source/graalvm-0.24-linux-amd64-jdk8.tar.gz  dest=" + SOURCE_DIR + "\""
    os.system(cmd)

def extractSourceTars():
    cmd="ansible -i hosts.ini myCluster  -m shell -a \"cd  " + DEPLOYMENT_DIR + " ; tar xvf " + SOURCE_DIR + "/zookeeper-3.4.10.tar.gz ; mv zookeeper-3.4.10 zookeeper\""
    os.system(cmd)
    cmd="ansible -i hosts.ini myCluster  -m shell -a \"cd " + DEPLOYMENT_DIR + " ; tar xvf " + SOURCE_DIR + "/hadoop-2.8.0.tar.gz ; mv hadoop-2.8.0 hadoop\""
    os.system(cmd)
    cmd="ansible -i hosts.ini myCluster  -m shell -a \"cd " + DEPLOYMENT_DIR + " ; tar xvf " + SOURCE_DIR + "/spark-2.1.1-bin-hadoop2.7.tgz ; mv spark-2.1.1-bin-hadoop2.7 spark \""
    os.system(cmd)
    cmd="ansible -i hosts.ini myCluster  -m shell -a \"cd " + DEPLOYMENT_DIR +"  ; tar xvf " + SOURCE_DIR + "/jdk-8u131-linux-x64.tar.gz ; mv jdk1.8.0_131 java  \""
    os.system(cmd)
    cmd="ansible -i hosts.ini myCluster  -m shell -a \"cd " + DEPLOYMENT_DIR +"  ; tar xvf " + SOURCE_DIR + "/graalvm-0.24-linux-amd64-jdk8.tar.gz ; mv graalvm-0.24 graal  \""
    os.system(cmd)

def words(stringIterable):
    #upcast the argument to an iterator, if it's an iterator already, it stays the same
    lineStream = iter(stringIterable)
    for line in lineStream: #enumerate the lines
        for word in line.split(): #further break them down
            yield word

    
def prepZooConfig():
    print "[INFO ] prepping configs ."
    zoo_cfg="""tickTime=2000  
initLimit=10 
syncLimit=5 
dataDir=""" + ZOO_DATA_DIR + """
clientPort=2181 
"""

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
           zoo_cfg += "server." + str(zoo_cnt) + "=" + line + ":2888:3888\n"

    with open("zookeeper.cfg", "w") as text_file:
         text_file.write("{}".format(zoo_cfg))

    zoo_setup_yml="""---
- hosts: zookeeper
  environment:
        ZOOKEEPER_HOME: """ + ZOOKEEPER_HOME  + """
        ZOOKEEPER_CONF_DIR: """ + ZOOKEEPER_CONF_DIR + """
        ZOO_LOG_DIR: """ + ZOO_LOG_DIR + """
  tasks:
  - name: create needed directories
    shell: mkdir -p """ + ZOO_LOG_DIR  + """ """ + ZOOKEEPER_CONF_DIR + """  """ + ZOO_DATA_DIR + """
  - name:  copy zookeeper config files
    copy: src={{ item.src }} dest={{ item.dest }}
    with_items:
        - { src: 'log4j.properties', dest:  $ZOOKEEPER_HOME/conf }
        - { src: 'zookeeper.cfg', dest: $ZOOKEEPER_CONF_DIR }
  - name: set zookeeper myid
    shell: echo {{ play_hosts.index(inventory_hostname) + 1 }}   >  \"""" + ZOO_DATA_DIR + """/myid\"
"""
    with open("zoo-setup.yml", "w") as text_file:
         text_file.write("{}".format(zoo_setup_yml))

    zoo_start_yml="""---
- hosts: zookeeper
  environment:
        JAVA_HOME: """ + JAVA_HOME + """
        ZOOKEEPER_HOME: """ + ZOOKEEPER_HOME + """
        ZOOKEEPER_CONF_DIR: """ + ZOOKEEPER_CONF_DIR + """
        ZOO_LOG_DIR: """ + ZOO_LOG_DIR + """
  tasks:
  - name: start zookeeper
    shell: $ZOOKEEPER_HOME/bin/zkServer.sh start $ZOOKEEPER_CONF_DIR/zookeeper.cfg

"""
    with open("zoo-start.yml", "w") as text_file:
         text_file.write("{}".format(zoo_start_yml))

    zoo_stop_yml="""---
- hosts: zookeeper
  environment:
        JAVA_HOME: """ + JAVA_HOME + """
        ZOOKEEPER_HOME: """ + ZOOKEEPER_HOME + """
        ZOOKEEPER_CONF_DIR: """ + ZOOKEEPER_CONF_DIR + """
        ZOO_LOG_DIR: """ + ZOO_LOG_DIR + """
  tasks:
  - name: kill zookeeper processes
    shell: pkill -f org.apache.zookeeper.server.quorum.QuorumPeerMain
"""
    with open("zoo-stop.yml", "w") as text_file:
         text_file.write("{}".format(zoo_stop_yml))


    #cmd="ansible -i hosts.ini zookeeper  -m copy -a \"src=/scratch/balakcha/ansible/zookeeper.cfg dest="+ZOOKEEPER_CONF_DIR+"  \""
    #os.system(cmd)
    #cmd="ansible-playbook -i hosts.ini zookeeper.yml"
    #os.system(cmd)

def prepHdfsConfig():
    print "[INFO ] prepping hdfs configs ."
    core_site="""<configuration>
          <property>
                <name>fs.defaultFS</name>
                <value>hdfs://"""

    found = False
    hdfs_cnt = 0
    with open("hosts.ini", "r") as f:
       lines = f.read().splitlines()
       for line in lines:
         if '[hdfs]' in line :
               found = True
               continue
         if not line.strip() : 
            found = False
            continue

         if found:
              hdfs_cnt += 1
              if hdfs_cnt == 2 :
                break
              # print line
              core_site += line + """:9001</value>
          </property>
</configuration>
"""

    with open("core-site.xml", "w") as text_file:
         text_file.write("{}".format(core_site))

    hdfs_site="""<configuration>
                <property>
                        <name>dfs.namenode.secondary.http-address</name>
                        <value>0.0.0.0:50090</value>
                </property>
                <property>
                        <name>dfs.namenode.secondary.https-address</name>
                        <value>0.0.0.0:50091</value>
                </property>
                <property>
                        <name>dfs.data.dir</name>
                        <value>""" + HDFS_DATA + """/data</value>
                </property>
                <property>
                        <name>dfs.namenode.name.dir</name>
                        <value>file://""" + HDFS_DATA  + """/name</value>
                </property>
</configuration>
"""
    with open("hdfs-site.xml", "w") as text_file:
         text_file.write("{}".format(hdfs_site))

    mapred_site="""<configuration>
        <property>
                <name>mapreduce.framework.name</name>
                <value>yarn</value>
         </property>
        <property>
          <name>mapred.job.tracker</name>
          <value>"""

    found = False
    hdfs_cnt = 0
    with open("hosts.ini", "r") as f:
       lines = f.read().splitlines()
       for line in lines:
         if '[hdfs]' in line :
               found = True
               continue
         if not line.strip() :
            found = False
            continue

         if found:
              hdfs_cnt += 1
              if hdfs_cnt == 2 :
                break
              # print line
              mapred_site += line + """:9002 </value>
        </property>
        <property>
                    <name>mapred.tasktracker.map.tasks.maximum</name>
                    <value>16</value>
         </property>
          <property>
             <name>mapred.tasktracker.reduce.tasks.maximum</name>
             <value>16</value>
        </property>
</configuration>
"""

    with open("mapred-site.xml", "w") as text_file:
         text_file.write("{}".format(mapred_site))


    yarn_site="""<configuration>
	<property>
		<name>yarn.resourcemanager.hostname</name>
		<value>"""
    found = False
    hdfs_cnt = 0
    with open("hosts.ini", "r") as f:
       lines = f.read().splitlines()
       for line in lines:
         if '[hdfs]' in line :
               found = True
               continue
         if not line.strip() :
            found = False
            continue

         if found:
              hdfs_cnt += 1
              if hdfs_cnt == 2 :
                break
              # print line
              yarn_site += line + """</value>
	</property>
	<property>
		<name>yarn.resourcemanager.bind-host</name>
		<value>0.0.0.0</value>
	</property>
	<property>
		<name>yarn.nodemanager.bind-host</name>
		<value>0.0.0.0</value>
	</property>
	<property>
	    <name>yarn.log-aggregation-enable</name>
	    <value>true</value>
	</property>
	<property>
	    <name>yarn.nodemanager.resource.memory-mb</name>
	    <value>128000</value>
	</property>
	<property>
	    <name>yarn.scheduler.maximum-allocation-mb</name>
	    <value>128000</value>
	</property>
	<property>
	    <name>yarn.nodemanager.resource.cpu-vcores</name>
	    <value>32</value>
	</property>
        <property>
                <name>yarn.nodemanager.aux-services</name>
        	<value>mapreduce_shuffle</value>
        </property>
        <property>
                 <name>yarn.nodemanager.aux-services.mapreduce.shuffle.class</name>
        	<value>org.apache.hadoop.mapred.ShuffleHandler</value>
        </property>
</configuration>
"""

    with open("yarn-site.xml", "w") as text_file:
         text_file.write("{}".format(yarn_site))

    master_v=""
    found = False
    hdfs_cnt = 0
    with open("hosts.ini", "r") as f:
       lines = f.read().splitlines()
       for line in lines:
         if '[namenode]' in line :
               found = True
               continue
         if not line.strip() :
            found = False
            continue

         if found:
              hdfs_cnt += 1
              if hdfs_cnt == 2 :
                break
              # print line
              master_v += line
    with open("master", "w") as text_file:
         text_file.write("{}".format(master_v))

    slaves_v=""
    found = False
    hdfs_cnt = 0
    with open("hosts.ini", "r") as f:
       lines = f.read().splitlines()
       for line in lines:
         if '[hdfs]' in line :
               found = True
               continue
         if not line.strip() :
            found = False
            continue

         if found:
              # print line
              slaves_v += line + "\n"

    with open("slaves", "w") as text_file:
         text_file.write("{}".format(slaves_v))


    hdfs_setup_yml="""---
- hosts: hdfs
  environment:
      HADOOP_HOME:  """ + HADOOP_HOME + """
      HADOOP_CONF_DIR:  """ + HADOOP_CONF_DIR + """
  tasks:
  - name: create needed directories
    shell: mkdir -p """ +  HDFS_DATA +  """ """ + HDFS_LOG_DIR + """ """ + HADOOP_CONF_DIR + """
  - name:  copy hdfs config files
    copy: src={{ item.src }} dest={{ item.dest }}
    with_items:
        - { src: 'core-site.xml', dest: $HADOOP_CONF_DIR }
        - { src: 'hdfs-site.xml', dest: $HADOOP_CONF_DIR }
        - { src: 'mapred-site.xml', dest: $HADOOP_CONF_DIR  }
        - { src: 'yarn-site.xml', dest: $HADOOP_CONF_DIR  }
        - { src: 'master', dest: $HADOOP_CONF_DIR  }
        - { src: 'slaves', dest: $HADOOP_CONF_DIR  }
        - { src: 'hadoop-env.sh', dest: $HADOOP_CONF_DIR  }
"""
    with open("hdfs-setup.yml", "w") as text_file:
         text_file.write("{}".format(hdfs_setup_yml))

    #cmd="ansible-playbook -i hosts.ini hdfs.yml"
    #os.system(cmd)

    hdfs_start_yml="""---
- hosts: namenode
  environment:
      JAVA_HOME : """ + JAVA_HOME + """
      HADOOP_HOME:  """ + HADOOP_HOME + """
      HADOOP_CONF_DIR:  """ + HADOOP_CONF_DIR + """
  tasks:
  - name: start hdfs
    shell: $HADOOP_HOME/sbin/start-dfs.sh --config $HADOOP_CONF_DIR
  - name: start yarn
    shell: $HADOOP_HOME/sbin/start-yarn.sh --config $HADOOP_CONF_DIR
"""
    with open("hdfs-start.yml", "w") as text_file:
         text_file.write("{}".format(hdfs_start_yml))

    hdfs_stop_yml="""---
- hosts: namenode
  environment:
      JAVA_HOME : """ + JAVA_HOME + """
      HADOOP_HOME:  """ + HADOOP_HOME + """
      HADOOP_CONF_DIR:  """ + HADOOP_CONF_DIR + """
  tasks:
  - name: stop hdfs
    command: "{{ item }} "
    with_items:
    - $HADOOP_HOME/sbin/stop-yarn.sh
    - $HADOOP_HOME/sbin/stop-dfs.sh
"""
    with open("hdfs-stop.yml", "w") as text_file:
         text_file.write("{}".format(hdfs_stop_yml))

def hdfsFormatOneTime():
    print "[INFO ] formatting hdfs "
    cmd="ansible -i hosts.ini namenode  -m shell -a \"export JAVA_HOME="+JAVA_HOME+" ; cd "+HADOOP_HOME+"; yes Y | ./bin/hdfs --config "+HADOOP_CONF_DIR+" namenode -format \""
    os.system(cmd)

def prepSparkConfig():
    print "[INFO ] prepping spark configs ."
    spark_master_v=""

    found = False
    spark_cnt = 0
    with open("hosts.ini", "r") as f:
       lines = f.read().splitlines()
       for line in lines:
         if '[namenode]' in line :
               found = True
               continue
         if not line.strip() :
            found = False
            continue

         if found:
              spark_cnt += 1
              if spark_cnt == 2 :
                break
              # print line
              spark_master_v += line
    with open("spark_master", "w") as text_file:
         text_file.write("{}".format(spark_master_v))

    els = ET.parse("core-site.xml").getroot()
    for prop in els.findall("property"):
       name = prop.find("name")
       value = prop.find("value")
       if name.text == "fs.defaultFS":
          hdfs_defaultFS = value.text
          #print "hi bala " + value.text

    append_to_defaults_1 = "spark.history.fs.logDirectory   " + hdfs_defaultFS + "/spark/history \n"
    append_to_defaults_2 = "spark.eventLog.dir   " + hdfs_defaultFS + "/spark/eventlog \n"

    shutil.copyfile("spark-defaults.conf.template", "spark-defaults.conf")
    with open("spark-defaults.conf", "a") as myfile:
        #myfile.write("spark.history.fs.logDirectory  hdfs://swarm001.ib.swarm:9001/spark/history")
        myfile.write(append_to_defaults_1)
        myfile.write(append_to_defaults_2)

    shutil.copyfile("spark.metrics.properties.template", "spark.metrics.properties")
    shutil.copyfile("spark-env.sh.template", "spark-env.sh")

    spark_setup_yml="""---
- hosts: spark
  environment:
      JAVA_HOME: """ + JAVA_HOME  + """
      HADOOP_HOME: """ + HADOOP_HOME + """
      HADOOP_CONF_DIR: """  + HADOOP_CONF_DIR + """
      SPARK_HOME: """ + SPARK_HOME + """
      SPARK_CONF_DIR: """ + SPARK_CONF_DIR + """
      SPARK_MASTER_HOST: \"""" + spark_master_v + """\"
  tasks:
  - name: create needed directories
    shell: mkdir -p """ + SPARK_CONF_DIR +  """  """ + SPARK_LOG_DIR + """
  - name:  copy spark config files
    copy: src={{ item.src }} dest={{ item.dest }}
    with_items:
        - { src: 'dot-bashrc', dest: ~/.bashrc  , backup: yes }
        - { src: 'spark-env.sh', dest: $SPARK_CONF_DIR }
        - { src: 'spark-defaults.conf', dest: $SPARK_CONF_DIR  }
        - { src: 'spark_master', dest: $SPARK_CONF_DIR  }
        - { src: 'spark.metrics.properties', dest: $SPARK_CONF_DIR  }
        - { src: 'slaves', dest: $SPARK_CONF_DIR  }
- hosts: sparkMaster
  environment:
      JAVA_HOME: """ + JAVA_HOME  + """
      HADOOP_HOME: """ + HADOOP_HOME + """
      HADOOP_CONF_DIR: """  + HADOOP_CONF_DIR + """
      SPARK_HOME: """ + SPARK_HOME + """
      SPARK_CONF_DIR: """ + SPARK_CONF_DIR + """
      SPARK_MASTER_HOST: \"""" + spark_master_v + """\"
  tasks:
  - name: create hdfs pre requisite directories
    command: "{{ item }} "
    with_items:
    - $HADOOP_HOME/bin/hdfs dfs -mkdir /spark
    - $HADOOP_HOME/bin/hdfs dfs -mkdir /spark/history
    - $HADOOP_HOME/bin/hdfs dfs -mkdir /spark/eventlog
"""

    with open("spark-setup.yml", "w") as text_file:
         text_file.write("{}".format(spark_setup_yml))

    spark_start_yml="""---
- hosts: sparkMaster
  environment:
      JAVA_HOME: """ + JAVA_HOME  + """
      HADOOP_HOME: """ + HADOOP_HOME + """
      HADOOP_CONF_DIR: """  + HADOOP_CONF_DIR + """
      SPARK_HOME: """ + SPARK_HOME + """
      SPARK_CONF_DIR: """ + SPARK_CONF_DIR + """
      SPARK_MASTER_HOST: \"""" + spark_master_v + """\"
  tasks:
  - name: start spark
    command: "{{ item }} "
    with_items:
    - $SPARK_HOME/sbin/start-master.sh  -h $SPARK_MASTER_HOST
    - $SPARK_HOME/sbin/start-slaves.sh
"""

    with open("spark-start.yml", "w") as text_file:
         text_file.write("{}".format(spark_start_yml))

    spark_stop_yml="""---
- hosts: sparkMaster
  environment:
      JAVA_HOME: """ + JAVA_HOME  + """
      HADOOP_HOME: """ + HADOOP_HOME + """
      HADOOP_CONF_DIR: """  + HADOOP_CONF_DIR + """
      SPARK_HOME: """ + SPARK_HOME + """
      SPARK_CONF_DIR: """ + SPARK_CONF_DIR + """
      SPARK_MASTER_HOST: \"""" + spark_master_v + """\"
  tasks:
  - name: stop spark
    command: "{{ item }} "
    with_items:
    - $SPARK_HOME/sbin/stop-slaves.sh
    - sleep 10
    - $SPARK_HOME/sbin/stop-master.sh
"""

    with open("spark-stop.yml", "w") as text_file:
         text_file.write("{}".format(spark_stop_yml))

    dot_bashrc_v="""export JAVA_HOME=""" + JAVA_HOME  + """
export CLUSTER_CONF_DIR=""" + CLUSTER_CONF_DIR + """
export ZOOKEEPER_HOME=""" + ZOOKEEPER_HOME + """
export ZOOKEEPER_CONF_DIR=""" + ZOOKEEPER_CONF_DIR + """
export HADOOP_HOME=""" + HADOOP_HOME + """
export HADOOP_CONF_DIR="""  + HADOOP_CONF_DIR + """
export SPARK_HOME=""" + SPARK_HOME + """
export SPARK_CONF_DIR=""" + SPARK_CONF_DIR + """
export SPARK_MASTER_HOST=""" + spark_master_v + """
export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$SPARK_HOME/bin:$ZOOKEEPER_HOME/bin:/usr/lib64/qt-3.3/bin:/usr/local/bin:/bin:/usr/bin:/usr/local/sbin:/usr/sbin:/sbin:/opt/ibutils/bin:/sbin:/usr/sbin
"""

    with open("dot-bashrc", "w") as text_file:
         text_file.write("{}".format(dot_bashrc_v))


if __name__ == "__main__":
    # by default do nothing
    op = "nothingToDo"
    if len(sys.argv) == 2:
        op = sys.argv[1]

    ts=datetime.datetime.now().strftime('%Y-%m-%d_%H:%M')

    if (op == "adhoc"):
        print "[INFO] ========== running adhoc comds   =========="
        cmd="ansible -i hosts.ini myCluster  -m copy -a \"src=../source/graalvm-0.24-linux-amd64-jdk8.tar.gz  dest=" + SOURCE_DIR + "\""
        os.system(cmd)
        cmd="ansible -i hosts.ini myCluster  -m shell -a \"cd " + DEPLOYMENT_DIR +"  ; tar xvf " + SOURCE_DIR + "/graalvm-0.24-linux-amd64-jdk8.tar.gz ; mv graalvm-0.24 graal  \""
        os.system(cmd)
    elif (op == "just_ping"):
        print "[INFO] ========== pinging the hosts   =========="
        justPing()
    elif (op == "new_set_of_hosts"):
        print "[INFO] ========== pinging the hosts   =========="
        justPing()
        copySourceTars()
        extractSourceTars()
        prepZooConfig()
        prepHdfsConfig()
        prepSparkConfig()
        print """ [NEXT STEPS ] : please run cluster-admin.py "new_set_of_hosts"  
"""
    else:
        print "[ERROR] Unrecognized option (can be \"adhoc\", \"just_ping\" , \"copy_over_tars\" , \"new_set_of_hosts\" ): "+op


