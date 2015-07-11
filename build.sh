#!/bin/bash -e

base_dir=$(dirname $0)
projects_dir=$base_dir/projects  # Clone all projects into this directory
if [ ! -d $projects_dir ]; then
    mkdir -p $projects_dir
fi

# Detect jdk version
jdk=`javac -version 2>&1 | cut -d ' ' -f 2`
ver=`echo $jdk | cut -d '.' -f 2`
if (( $ver > 7 )); then
    echo "Found jdk version $jdk"
    echo "You should only build with jdk 1.7 or below."
    exit 1
fi

GIT_MODE="git@github.com:"
while [ $# -gt 0 ]; do
  OPTION=$1
  case $OPTION in
    --update)
      UPDATE="yes"
      shift
      ;;
    --aws)
      GIT_MODE="https://github.com/"
      shift
      ;;
    *)
      break
      ;;
  esac
done

# Default JAVA_HOME for EC2
if [ -z "$JAVA_HOME" ]; then
    export JAVA_HOME=/usr/lib/jvm/java-7-openjdk-amd64
fi

# Default gradle for local gradle download, e.g. on EC2
if [ ! `which gradle` ]; then
    export PATH=$projects_dir/`find . | grep gradle-.*/bin$`:$PATH
fi

KAFKA_VERSION=0.8.2.0

if [ ! -d $projects_dir/kafka ]; then
    echo "Cloning Kafka"
    # temporarily pull Kafka from confluent's fork so we can use our system_test
    # git clone http://git-wip-us.apache.org/repos/asf/kafka.git $projects_dir/kafka
    git clone https://github.com/confluentinc/kafka $projects_dir/kafka
fi

pushd $projects_dir/kafka

if [ "x$UPDATE" == "xyes" ]; then
    echo "Updating Kafka"
    git fetch origin
fi

#git checkout tags/$KAFKA_VERSION
git checkout system-tests

# FIXME we should be installing the version of Kafka we built into the local
# Maven repository and making sure we specify the right Kafka version when
# building our own projects. Currently ours link to whatever version of Kafka
# they default to, which should work ok for now.
echo "Building Kafka"
KAFKA_BUILD_OPTS=""
if [ "x$SCALA_VERSION" != "x" ]; then
    KAFKA_BUILD_OPTS="$KAFKA_BUILD_OPTS -PscalaVersion=$SCALA_VERSION"
fi
if [ ! -e gradle/wrapper/ ]; then
    gradle
fi
./gradlew $KAFKA_BUILD_OPTS jar
popd

function build_maven_project() {
    NAME=$1
    URL=$2
    # The build target can be specified so that shared libs get installed and
    # can be used in the build process of applications, but applications only
    # need to build enough to be tested.
    BUILD_TARGET=$3
    BRANCH=${4:-master}

    if [ ! -d $projects_dir/$NAME ]; then
        echo "Cloning $NAME"
        git clone $URL $projects_dir/$NAME
    fi

    # Turn off tests for the build because some of these are local integration
    # tests that take a long time. This shouldn't be a problem since these
    # should be getting run elsewhere.
    BUILD_OPTS="-DskipTests"
    if [ "x$SCALA_VERSION" != "x" ]; then
        BUILD_OPTS="$BUILD_OPTS -Dkafka.scala.version=$SCALA_VERSION"
    fi

    pushd $projects_dir/$NAME

    if [ "x$UPDATE" == "xyes" ]; then
        echo "Updating $NAME"
        git pull origin
    fi

    if [ "x$BRANCH" != "xmaster" ]; then
        echo "Checking out branch $BRANCH"
        git branch --track $BRANCH origin/$BRANCH || true
        git checkout $BRANCH
    fi

    echo "Building $NAME"
    mvn $BUILD_OPTS $BUILD_TARGET
    popd
}

build_maven_project "common" "${GIT_MODE}confluentinc/common.git" "install"
build_maven_project "rest-utils" "${GIT_MODE}confluentinc/rest-utils.git" "install"
build_maven_project "schema-registry" "${GIT_MODE}confluentinc/schema-registry.git" "install"
build_maven_project "kafka-rest" "${GIT_MODE}confluentinc/kafka-rest.git" "package"
build_maven_project "camus" "${GIT_MODE}confluentinc/camus.git" "package" "confluent-master"
