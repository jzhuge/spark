#!/bin/bash

set -e

# configure the environment to build SparkR
if ! grep 'cran' /etc/apt/sources.list; then
  sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys E084DAB9
  sudo echo deb http://cran.r-project.org/bin/linux/ubuntu/ trusty/ | sudo tee -a /etc/apt/sources.list
fi

sudo apt-get update -y
sudo apt-get install -y \
    r-base \
    r-base-dev \
    r-cran-plyr \
    r-cran-ggplot2

if [[ ! -d ~/.R ]]; then
  mkdir ~/.R
  echo 'R_LIBS_USER=~/.R:/usr/lib/R/site-library' > ~/.Renviron
  echo 'options(repos=structure(c(CRAN="http://cran.rstudio.com")))' > ~/.Rprofile
  echo "PKG_CXXFLAGS = '-std=c++11'" > ~/.R/Makevars
  echo "install.packages(c('roxygen2', 'testthat'))" | R --no-save
fi

export NETFLIX_ENVIRONMENT=prod

# this is the version used for output files to distinguish this branch
BASE_VERSION=2.1.1
SPARK_VERSION=${BASE_VERSION}-nflx
# default to unstable if BUILD_VERSION is not in the environment
BUILD_VERSION=${BUILD_VERSION:-${BASE_VERSION}-unstable}
HADOOP_VERSION_PROFILE=hadoop-2.7
HADOOP_VERSION=2.7.3

wget http://www-us.apache.org/dist/maven/maven-3/3.3.9/binaries/apache-maven-3.3.9-bin.tar.gz
tar -xf apache-maven-3.3.9-bin.tar.gz
export M2_HOME=${WORKSPACE}/apache-maven-3.3.9
export PATH=${M2_HOME}/bin:${PATH}

# copy log4j configurations so they are included in the assembly
cp netflix/log4j-defaults.properties core/src/main/resources/org/apache/spark/
cp netflix/log4j-defaults-repl.properties core/src/main/resources/org/apache/spark/
cp netflix/metrics-site.xml core/src/main/resources/

# avoid stale compile servers causing problems
build/zinc-0.3.9/bin/zinc -shutdown || echo "No zinc server running."

dev/make-distribution.sh --tgz -P${HADOOP_VERSION_PROFILE} -Pmesos -Pyarn -Phive-thriftserver -Psparkr -Pkinesis-asl -Phadoop-provided -Dmaven.repo.local=${WORKSPACE}/.m2/repository 

rm -rf spark-*-bin-${HADOOP_VERSION}
tar -xf spark-${SPARK_VERSION}-bin-${HADOOP_VERSION}.tgz
rm -rf spark-${BUILD_VERSION}
mv spark-${SPARK_VERSION}-bin-${HADOOP_VERSION} spark-${BUILD_VERSION}

# Remove unnecessary files
rm -f spark-${BUILD_VERSION}/lib/spark-examples-*-hadoop${HADOOP_VERSION}.jar
rm -f spark-${BUILD_VERSION}/lib/spark-*-yarn-shuffle.jar
rm -f spark-${BUILD_VERSION}/CHANGES.txt
rm -f spark-${BUILD_VERSION}/LICENSE
rm -f spark-${BUILD_VERSION}/NOTICE
rm -f spark-${BUILD_VERSION}/README.md
rm -rf spark-${BUILD_VERSION}/examples/
rm -rf spark-${BUILD_VERSION}/conf/
rm -rf spark-${BUILD_VERSION}/data/
rm -rf spark-${BUILD_VERSION}/ec2/

# Add extra dependency jars
curl https://artifacts.netflix.com/libs-snapshots-local/netflix/atlas-hive/1.2.5/atlas-hive-1.2.5.jar -o spark-${BUILD_VERSION}/jars/atlas-hive-1.2.5.jar
cp ${WORKSPACE}/.m2/repository/org/datanucleus/datanucleus-core/3.2.10/datanucleus-core-3.2.10.jar spark-${BUILD_VERSION}/jars/
cp ${WORKSPACE}/.m2/repository/org/datanucleus/datanucleus-rdbms/3.2.9/datanucleus-rdbms-3.2.9.jar spark-${BUILD_VERSION}/jars/
cp ${WORKSPACE}/.m2/repository/org/datanucleus/datanucleus-api-jdo/3.2.6/datanucleus-api-jdo-3.2.6.jar spark-${BUILD_VERSION}/jars/

# Add spark-defaults.conf and spark-env.sh
mkdir -p ${WORKSPACE}/spark-${BUILD_VERSION}/conf
cp netflix/spark-defaults.conf ${WORKSPACE}/spark-${BUILD_VERSION}/conf/
cp netflix/spark-env.sh ${WORKSPACE}/spark-${BUILD_VERSION}/conf/

# Add Jenkins build number to the spark tar ball
echo ${BUILD_NUMBER} > ${WORKSPACE}/spark-${BUILD_VERSION}/BUILD

# Add run.py
cp netflix/run.py ${WORKSPACE}/spark-${BUILD_VERSION}/bin/run.py

# Overwrite dsespark-shell and dsespark-submit
cp bin/dsespark-* spark-${BUILD_VERSION}/bin/

# Update tarball
tar -czf spark-${BUILD_VERSION}.tgz spark-${BUILD_VERSION}
assume-role -a arn:aws:iam::219382154434:role/BDP_JENKINS_ROLE aws s3 cp spark-${BUILD_VERSION}.tgz s3://netflix-bigdataplatform/spark-builds/${BUILD_VERSION}/
assume-role -a arn:aws:iam::219382154434:role/BDP_JENKINS_ROLE aws s3 cp spark-${BUILD_VERSION}.tgz s3://netflix-bigdataplatform/spark-builds/${BUILD_VERSION}/spark-${BUILD_VERSION}-${BUILD_NUMBER}.tgz

