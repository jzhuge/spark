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
BASE_VERSION=2.3.2
# default to unstable if SPARK_VERSION and BUILD_VERSION are not in the environment
SPARK_VERSION=${SPARK_VERSION:-${BASE_VERSION}-unstable-$BUILD_NUMBER}
BUILD_VERSION=${BUILD_VERSION:-${BASE_VERSION}-unstable}
HADOOP_VERSION_PROFILE=hadoop-2.7
HADOOP_VERSION=2.7.3

# tag the release and push to the repository
git tag -a v$SPARK_VERSION -m "Version $SPARK_VERSION"
git push origin v$SPARK_VERSION

wget http://www-us.apache.org/dist/maven/maven-3/3.3.9/binaries/apache-maven-3.3.9-bin.tar.gz
tar -xf apache-maven-3.3.9-bin.tar.gz
export M2_HOME=${WORKSPACE}/apache-maven-3.3.9
export PATH=${M2_HOME}/bin:${PATH}

# copy log4j configurations so they are included in the assembly
cp netflix/log4j-defaults.properties core/src/main/resources/org/apache/spark/
cp netflix/log4j-defaults-repl.properties core/src/main/resources/org/apache/spark/
cp netflix/metrics-site.xml core/src/main/resources/

# avoid stale compile servers causing problems
export ZINC_PORT=3232

# set the maven version to include the build number
build/mvn versions:set -DnewVersion=$SPARK_VERSION -DgenerateBackupPoms=false

dev/make-distribution.sh --tgz -P${HADOOP_VERSION_PROFILE} -Pmesos -Pyarn -Phive-thriftserver -Psparkr -Pkinesis-asl -Phadoop-provided

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

# Add spark-defaults.conf and spark-env.sh
mkdir -p ${WORKSPACE}/spark-${BUILD_VERSION}/conf
cp netflix/spark-defaults.conf ${WORKSPACE}/spark-${BUILD_VERSION}/conf/
cp netflix/spark-env.sh ${WORKSPACE}/spark-${BUILD_VERSION}/conf/

# Add Jenkins build number to the spark tar ball
echo ${BUILD_NUMBER} > ${WORKSPACE}/spark-${BUILD_VERSION}/BUILD

# Add run.py
cp netflix/run.py ${WORKSPACE}/spark-${BUILD_VERSION}/bin/run.py
cp netflix/run-history.py ${WORKSPACE}/spark-${BUILD_VERSION}/bin/run-history.py

# Update tarball
tar -czf spark-${BUILD_VERSION}.tgz spark-${BUILD_VERSION}
assume-role -a arn:aws:iam::219382154434:role/BDP_JENKINS_ROLE aws s3 cp spark-${BUILD_VERSION}.tgz s3://netflix-bigdataplatform/spark-builds/${BUILD_VERSION}/
assume-role -a arn:aws:iam::219382154434:role/BDP_JENKINS_ROLE aws s3 cp spark-${BUILD_VERSION}.tgz s3://netflix-bigdataplatform/spark-builds/${BUILD_VERSION}/spark-${BUILD_VERSION}-${BUILD_NUMBER}.tgz

