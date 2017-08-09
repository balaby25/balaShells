#!/bin/bash
# Usage : sbt-generate-jars.sh  <hq1|hq5>

if [ \(  "$1" = "hq1" \) -o \( "$1" = "hq5" \) ] ; then 
   application_name=$1
   #echo "creating sbt directories for $application_name"
else 
   echo 'Usage : sbt-generate-jars.sh <hq1|hq5>'
   exit 1
fi

export DEPLOYMENT_DIR=/scratch/balakcha
export ANSIBLE_DIR=$DEPLOYMENT_DIR/ansible
export SBT_HOME=$DEPLOYMENT_DIR/sbt
export SBT_WORKSPACE_DIR=$DEPLOYMENT_DIR/sbtWorkspace
export http_proxy=http://www-proxy.us.oracle.com:80
export https_proxy=http://www-proxy.us.oracle.com:80

if [[ ! -x "${SBT_HOME}/bin/sbt"  ]]; then
   echo "sbt binary is not there or is not executable. check and rerun"
   exit 1
fi

if [[ -d ${SBT_WORKSPACE_DIR} ]]; then
    rm -rf ${SBT_WORKSPACE_DIR} 
    mkdir -p $SBT_WORKSPACE_DIR
else 
    mkdir -p $SBT_WORKSPACE_DIR
fi

cd $SBT_WORKSPACE_DIR

echo "creating $application_name SBT project directory beneath $SBT_WORKSPACE_DIR"

mkdir -p ${application_name}/src/{main,test}/{java,resources,scala}
mkdir ${application_name}/lib ${application_name}/project ${application_name}/target

#---------------------------------
# create an initial build.sbt file
#---------------------------------
TIME_STRING=$(date +"%Y%m%d_%H%M%S")
echo "name := \"$application_name\"

version := \"$TIME_STRING\"

organization := \"com.oracle.tpch1gspark\"

scalaVersion := \"2.11.8\"

mainClass in (Compile, packageBin) := Some(\"com.oracle.tpch1gspark.spark.$application_name\")

libraryDependencies ++= Seq(
\"org.apache.spark\" %% \"spark-core\" % \"2.1.0\" % \"provided\",
\"org.apache.spark\" %% \"spark-sql\" % \"2.1.0\" % \"provided\"
)
" > ${application_name}/build.sbt

echo "
addSbtPlugin(\"com.eed3si9n\" % \"sbt-assembly\" % \"0.14.3\")
" > ${application_name}/project/assembly.sbt

echo "sbt.version=0.13.15" > ${application_name}/project/build.properties

cp $ANSIBLE_DIR/$application_name.scala  $SBT_WORKSPACE_DIR/$application_name/src/main/scala/

echo " doing sbt  assembly .. this step would do many downloads like maven etc and hence would take time, particularly if this is first time in the env  "
cd $SBT_WORKSPACE_DIR/$application_name
$SBT_HOME/bin/sbt --batch assembly

echo "copying $application_name-assembly-$TIME_STRING.jar to $ANSIBLE_DIR"
cp  $SBT_WORKSPACE_DIR/$application_name/target/scala-2.11/$application_name-assembly-$TIME_STRING.jar  $ANSIBLE_DIR
cp  $SBT_WORKSPACE_DIR/$application_name/target/scala-2.11/$application_name-assembly-$TIME_STRING.jar  $ANSIBLE_DIR/$application_name.jar

echo "copying $ANSIBLE_DIR/$application_name.scala  to $ANSIBLE_DIR - just in case  for future refernce"
cp  $ANSIBLE_DIR/$application_name.scala  $ANSIBLE_DIR/$application_name.scala.$TIME_STRING

#echo "updating $TPCH1G_DEPLOYMENT_DIR/hadoop-spark-submit.sh to use $application_name-assembly-$TIME_STRING.jar "
#sed -i -- "s/getfromsbt.$application_name/$application_name-assembly-$TIME_STRING.jar/" $TPCH1G_DEPLOYMENT_DIR/hadoop-spark-submit.sh

