#!/bin/bash -e

base_dir=$(dirname $0)
projects_dir=$base_dir/projects  # Clone all projects into this directory
if [ ! -d $projects_dir ]; then
    mkdir -p $projects_dir
fi

# Detect jdk version
jdk=`javac -version 2>&1 | cut -d ' ' -f 2`
ver=`echo $jdk | cut -d '.' -f 2`
if (( $ver > 6 )); then
    echo "Found jdk version $jdk"
    echo "You should only build with jdk 1.6 or below."
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
    export JAVA_HOME=/usr/lib/jvm/java-6-openjdk-amd64
fi

# Default gradle for local gradle download, e.g. on EC2
if [ ! `which gradle` ]; then
    export PATH=$projects_dir/`find . | grep gradle-.*/bin$`:$PATH
fi

function kafka_dirname() {
    version=$1

    if [ "x$version" == "xtrunk" ]; then
        kafka_dir=$projects_dir/kafka
    else
        kafka_dir=$projects_dir/kafka-$version
    fi

    echo $kafka_dir
}

function checkout_kafka() {
    # After running this function, all specified branches will be available in the projects directory to be built
    kafka_versions=$@
    trunk_dir=`kafka_dirname trunk`

    # Get kafka if we don't already have it
    if [ ! -d $trunk_dir ]; then
        echo "Cloning Kafka"
        git clone http://git-wip-us.apache.org/repos/asf/kafka.git $trunk_dir

        pushd $trunk_dir
        git reset HEAD --hard
        popd
    fi

    # Update branches, tags if necessary
    if [ "x$UPDATE" == "xyes" ]; then
        pushd $trunk_dir
        echo "Updating Kafka"
        git fetch origin
        popd
    fi

    # Make one copy of kafka directory per version (aka tag/branch)
    for version in $kafka_versions; do
        kafka_dir=`kafka_dirname $version`

        if [ ! -d $kafka_dir ]; then
            cp -r $trunk_dir $kafka_dir

            pushd $kafka_dir
            git checkout tags/$version
            popd
        fi
    done
}

function build_kafka() {
    # build the various version of kafka that we want available
    # if UPDATE flag is set, we assume non-trunk versions are already built,
    # and only build trunk

    versions=$@

    # FIXME we should be installing the version of Kafka we built into the local
    # Maven repository and making sure we specify the right Kafka version when
    # building our own projects. Currently ours link to whatever version of Kafka
    # they default to, which should work ok for now.
    for version in $versions; do
        kafka_dir=`kafka_dirname $version`

        pushd $kafka_dir
        echo "Building Kafka $version"
        KAFKA_BUILD_OPTS=""
        if [ "x$SCALA_VERSION" != "x" ]; then
            KAFKA_BUILD_OPTS="$KAFKA_BUILD_OPTS -PscalaVersion=$SCALA_VERSION"
        fi
        if [ ! -e gradle/wrapper/ ]; then
            gradle
        fi

        ./gradlew $KAFKA_BUILD_OPTS jar
        popd
    done
}

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

KAFKA_VERSIONS="trunk 0.8.1.1 0.8.2.0 0.8.2.1"
checkout_kafka $KAFKA_VERSIONS
build_kafka $KAFKA_VERSIONS

#build_maven_project "common" "${GIT_MODE}confluentinc/common.git" "install"
#build_maven_project "rest-utils" "${GIT_MODE}confluentinc/rest-utils.git" "install"
#build_maven_project "schema-registry" "${GIT_MODE}confluentinc/schema-registry.git" "install"
#build_maven_project "kafka-rest" "${GIT_MODE}confluentinc/kafka-rest.git" "package"
#build_maven_project "camus" "${GIT_MODE}confluentinc/camus.git" "package" "confluent-master"
