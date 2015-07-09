# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#!/bin/bash

set -e

if [ -z `which javac` ]; then
    apt-get -y update
    apt-get install -y software-properties-common python-software-properties
    add-apt-repository -y ppa:webupd8team/java
    apt-get -y update

    # Try to share cache. See Vagrantfile for details
    mkdir -p /var/cache/oracle-jdk7-installer
    if [ -e "/tmp/oracle-jdk7-installer-cache/" ]; then
        find /tmp/oracle-jdk7-installer-cache/ -not -empty -exec cp '{}' /var/cache/oracle-jdk7-installer/ \;
    fi

    /bin/echo debconf shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections
    apt-get -y install oracle-java7-installer oracle-java7-set-default

    if [ -e "/tmp/oracle-jdk7-installer-cache/" ]; then
        cp -R /var/cache/oracle-jdk7-installer/* /tmp/oracle-jdk7-installer-cache
    fi
fi

chmod a+rw /opt

projects_dir=/vagrant/projects
projects="kafka common rest-utils kafka-rest schema-registry camus"
for project in $projects; do
    if [ -h /opt/$project ]; then
        # reset the symlink
        rm -f /opt/$project
    fi
    echo "linking /opt/$project to $projects_dir/$project"
    ln -s $projects_dir/$project /opt/$project
done

# For EC2 nodes, we want to use /mnt, which should have the local disk. On local
# VMs, we can just create it if it doesn't exist and use it like we'd use
# /tmp. Eventually, we'd like to also support more directories, e.g. when EC2
# instances have multiple local disks.
if [ ! -e /mnt ]; then
    mkdir /mnt
fi
chmod a+rwx /mnt

# Install and configure CDH
pushd /opt/
if [ -h /opt/hadoop-cdh ]; then
    # reset symlink
    rm -f /opt/hadoop-cdh
fi
if [ ! -e /opt/hadoop-2.5.0-cdh5.3.0 ]; then
    wget http://archive.cloudera.com/cdh5/cdh/5/hadoop-2.5.0-cdh5.3.0.tar.gz
    tar xvzf hadoop-2.5.0-cdh5.3.0.tar.gz
fi
ln -s /opt/hadoop-2.5.0-cdh5.3.0 /opt/hadoop-cdh
popd

# Delete uncessary host binding so that datanode can connect to namenode
sed -i '/127.0.1.1/d' /etc/hosts

# Install and configure HDP
wget http://public-repo-1.hortonworks.com/HDP/ubuntu12/2.x/GA/2.2.0.0/hdp.list -O /etc/apt/sources.list.d/hdp.list
gpg --keyserver pgp.mit.edu --recv-keys B9733A7A07513CAD
gpg -a --export 07513CAD | apt-key add -
apt-get update
apt-get install -y hadoop hadoop-hdfs libhdfs0 hadoop-yarn hadoop-mapreduce hadoop-client openssl
echo "export JAVA_HOME=/usr/lib/jvm/java-7-oracle" >> /etc/hadoop/conf/hadoop-env.sh
echo "export HADOOP_CONF_DIR=/mnt" >> /etc/hadoop/conf/hadoop-env.sh
