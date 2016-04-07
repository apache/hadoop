# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

load hadoop-functions_test_helper

@test "hadoop_finalize (shellprofiles)" {
  HADOOP_IS_CYGWIN=false

  hadoop_shellprofiles_finalize () { testvar=shell; }
  hadoop_finalize_classpath () { true; }
  hadoop_finalize_libpaths () { true; }
  hadoop_finalize_hadoop_heap () { true; }
  hadoop_finalize_hadoop_opts () { true; }
  hadoop_translate_cygwin_path () { true; }

  hadoop_finalize

  [ "${testvar}" = "shell" ];
}

@test "hadoop_finalize (classpath)" {
  HADOOP_IS_CYGWIN=false

  hadoop_shellprofiles_finalize () { true; }
  hadoop_finalize_classpath () {  testvar=class; }
  hadoop_finalize_libpaths () { true; }
  hadoop_finalize_hadoop_heap () { true; }
  hadoop_finalize_hadoop_opts () { true; }
  hadoop_translate_cygwin_path () { true; }

  hadoop_finalize

  [ "${testvar}" = "class" ];
}

@test "hadoop_finalize (libpaths)" {
  HADOOP_IS_CYGWIN=false

  hadoop_shellprofiles_finalize () { true; }
  hadoop_finalize_classpath () {  true; }
  hadoop_finalize_libpaths () { testvar=libpaths; }
  hadoop_finalize_hadoop_heap () { true; }
  hadoop_finalize_hadoop_opts () { true; }
  hadoop_translate_cygwin_path () { true; }

  hadoop_finalize

  [ "${testvar}" = "libpaths" ];
}


@test "hadoop_finalize (heap)" {
  HADOOP_IS_CYGWIN=false

  hadoop_shellprofiles_finalize () { true; }
  hadoop_finalize_classpath () {  true; }
  hadoop_finalize_libpaths () { true; }
  hadoop_finalize_hadoop_heap () { testvar=heap; }
  hadoop_finalize_hadoop_opts () { true; }
  hadoop_translate_cygwin_path () { true; }

  hadoop_finalize

  [ "${testvar}" = "heap" ];
}

@test "hadoop_finalize (opts)" {
  HADOOP_IS_CYGWIN=false

  hadoop_shellprofiles_finalize () { true; }
  hadoop_finalize_classpath () {  true; }
  hadoop_finalize_libpaths () { true; }
  hadoop_finalize_hadoop_heap () { true; }
  hadoop_finalize_hadoop_opts () { testvar=opts; }
  hadoop_translate_cygwin_path () { true; }

  hadoop_finalize

  [ "${testvar}" = "opts" ];
}

@test "hadoop_finalize (cygwin prefix)" {
  HADOOP_IS_CYGWIN=false

  hadoop_shellprofiles_finalize () { true; }
  hadoop_finalize_classpath () {  true; }
  hadoop_finalize_libpaths () { true; }
  hadoop_finalize_hadoop_heap () { true; }
  hadoop_finalize_hadoop_opts () { true; }
  hadoop_translate_cygwin_path () {
    if [ $1 = HADOOP_HOME ]; then
      testvar=prefix;
    fi
  }

  hadoop_finalize

  [ "${testvar}" = "prefix" ];
}

@test "hadoop_finalize (cygwin conf dir)" {
  HADOOP_IS_CYGWIN=false

  hadoop_shellprofiles_finalize () { true; }
  hadoop_finalize_classpath () {  true; }
  hadoop_finalize_libpaths () { true; }
  hadoop_finalize_hadoop_heap () { true; }
  hadoop_finalize_hadoop_opts () { true; }
  hadoop_translate_cygwin_path () {
    if [ $1 = HADOOP_CONF_DIR ]; then
      testvar=confdir;
    fi
  }

  hadoop_finalize

  [ "${testvar}" = "confdir" ];
}

@test "hadoop_finalize (cygwin common)" {
  HADOOP_IS_CYGWIN=false

  hadoop_shellprofiles_finalize () { true; }
  hadoop_finalize_classpath () {  true; }
  hadoop_finalize_libpaths () { true; }
  hadoop_finalize_hadoop_heap () { true; }
  hadoop_finalize_hadoop_opts () { true; }
  hadoop_translate_cygwin_path () {
    if [ $1 = HADOOP_COMMON_HOME ]; then
      testvar=common;
    fi
  }

  hadoop_finalize

  [ "${testvar}" = "common" ];
}

@test "hadoop_finalize (cygwin hdfs)" {
  HADOOP_IS_CYGWIN=false

  hadoop_shellprofiles_finalize () { true; }
  hadoop_finalize_classpath () {  true; }
  hadoop_finalize_libpaths () { true; }
  hadoop_finalize_hadoop_heap () { true; }
  hadoop_finalize_hadoop_opts () { true; }
  hadoop_translate_cygwin_path () {
    if [ $1 = HADOOP_HDFS_HOME ]; then
      testvar=hdfs;
    fi
  }

  hadoop_finalize

  [ "${testvar}" = "hdfs" ];
}

@test "hadoop_finalize (cygwin yarn)" {
  HADOOP_IS_CYGWIN=false

  hadoop_shellprofiles_finalize () { true; }
  hadoop_finalize_classpath () {  true; }
  hadoop_finalize_libpaths () { true; }
  hadoop_finalize_hadoop_heap () { true; }
  hadoop_finalize_hadoop_opts () { true; }
  hadoop_translate_cygwin_path () {
    if [ $1 = HADOOP_YARN_HOME ]; then
      testvar=yarn;
    fi
  }

  hadoop_finalize

  [ "${testvar}" = "yarn" ];
}

@test "hadoop_finalize (cygwin mapred)" {
  HADOOP_IS_CYGWIN=false

  hadoop_shellprofiles_finalize () { true; }
  hadoop_finalize_classpath () {  true; }
  hadoop_finalize_libpaths () { true; }
  hadoop_finalize_hadoop_heap () { true; }
  hadoop_finalize_hadoop_opts () { true; }
  hadoop_translate_cygwin_path () {
    if [ $1 = HADOOP_MAPRED_HOME ]; then
      testvar=mapred;
    fi
  }

  hadoop_finalize

  [ "${testvar}" = "mapred" ];
}