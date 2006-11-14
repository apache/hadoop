#!/bin/sh

#
# packageNativeHadoop.sh - A simple script to help package native-hadoop libraries
#

#
# Note: 
# This script relies on the following environment variables to function correctly:
#  * BASE_NATIVE_LIB_DIR
#  * BUILD_NATIVE_DIR
#  * DIST_LIB_DIR
# All these are setup by build.xml.
#

TAR='tar -c'
UNTAR='tar -x'

# Copy the pre-built libraries in $BASE_NATIVE_LIB_DIR 
for platform in `ls $BASE_NATIVE_LIB_DIR`
do
  if test ! -e $DIST_LIB_DIR/$platform
  then
    mkdir -p $DIST_LIB_DIR/$platform
    echo "Created $DIST_LIB_DIR/$platform"
  fi
  echo "Copying libraries in $BASE_NATIVE_LIB_DIR/$platform to $DIST_LIB_DIR/$platform/"
  cd $BASE_NATIVE_LIB_DIR/$platform/
  $TAR *hadoop* | $UNTAR -C $DIST_LIB_DIR/$platform/
done  

# Copy the custom-built libraries in $BUILD_DIR
for platform in `ls $BUILD_NATIVE_DIR`
do
  if test ! -e $DIST_LIB_DIR/$platform
  then
    mkdir -p $DIST_LIB_DIR/$platform
    echo "Created $DIST_LIB_DIR/$platform"
  fi
  echo "Copying libraries in $BUILD_NATIVE_DIR/$platform/lib to $DIST_LIB_DIR/$platform/"
  cd $BUILD_NATIVE_DIR/$platform/lib
  $TAR *hadoop* | $UNTAR -C $DIST_LIB_DIR/$platform/
done  

