#!/usr/bin/env bash

# Deploy the built jar file to ../../dashboard-scripts/AWS-LB/bin.
# We want a local copy of core-with-deps.jar.
# We want the target directory to already exist. 
# We want the copied .jar to be excluded from git.

target=core-with-deps.jar
targetDir="../../dashboard-scripts/AWS-LB/bin"

# Make sure we have a file to copy.
if [ ! -e ${target} ]; then
    echo "Cant find source file: ${target}"
    exit 1
fi

# Make sure we know where we're going to copy.
if [ ! -d ${targetDir} ]; then
    echo "Can't find target directory: ${targetDir}"
    exit 1
fi

# Only copy if we won't be checking the huge .jar into git
if (cd ${targetDir}; git check-ignore ${target} >/dev/null); then
    cp -vp ${target} ${targetDir}/${target}
fi

