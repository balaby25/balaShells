
This is to bring up zookeeper + hadoop + spark multi node cluster

1. Decide DEPLOYMENT_DIRECTORY.  all cluster machines should have enough space since hdfs datafiles will reside here (ex. /scratch/balakcha)
2. all python scripts have shebang  /scratch/balakcha/python/bin/python2.7 . globally replace this to point to your python executable.
have used 2.7 and have not tested in other python versions.
3. install ansible in bastion host from which all the installation scripts will be fired. 
4. get the software binaries : scp  -r /net/slc09stm/scratch/balakcha/zooHdfsYarnSpark/source bastion_host:/scratch/<userid>/source
5. clone this repo such that all the scripts are in /scratch/your_userid/ansbile 
6. prepare hosts.ini. [myCluster] section is the superset of all other sections
7. call_all_pythons.sh -- review this before running . this script will give u an idea of the steps to follow 
8. cluster-admin.py "new_set_of_hosts"  will take about 10 minutes for all the components to start..so,  hang in there.
9. at this point zookeeper + hdfs + spark  is up
```
in bastion host :
wget https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm
rpm -ivh epel
yum install ansible (server machines dont need ansible)
mkdir /scratch/<your_userid>/ansbile   
git clone this repo and copy the scripts under ansbile folder
```

TPCH setup

```
in client machine:  cd /scratch/balakcha/ansible
./tpch_setup.py firstTimeTpch tpch_8g
in sparkMaster : cd LAUNCH_DIR; ./spark-submit-hq[1|5].py

```


Excuse below Annoying decisions / Hard Codings . If this repo gets used frequently, will fix/develop further
1. tpch source csv PATH is hardcoded to swarm path. update prep-tpch.yml.template if diff path
2. tpch hq1.scala, hq5.scala are hardcoded to run qry 50 times. therefore, the tpch python scripts are also hard coded for 50
3. hardcoded/default port numbers:
zookeeper : 2888, 3888, 2010
hdfs masternode : 9001
secondary namenode : 50090
namenode : 50091
mapred-site : 9002
SPARK_MASTER_WEBUI_PORT=18080
SPARK_WORKER_WEBUI_PORT=18081
SPARK_MASTER_PORT client mode : 7077
SPARK_MASTER_PORT cluster mode : 6066

 
```
ssh-copy-id -i $HOME/.ssh/id_rsa.pub <your-username>@localhost
```
