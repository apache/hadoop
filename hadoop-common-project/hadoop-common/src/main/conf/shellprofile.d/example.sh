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

#
# This is an example shell profile.  It does not do anything other than
# show an example of what the general structure and API of the pluggable
# shell profile code looks like.
#
#

#
#  First, register the profile:
#
# hadoop_add_profile example
#
#
# This profile name determines what the name of the functions will
# be. The general pattern is _(profilename)_hadoop_(capability).  There
# are currently four capabilities:
#  * init
#  * classpath
#  * nativelib
#  * finalize
#
# None of these functions are required.  Examples of all four follow...

#
# The _hadoop_init function is called near the very beginning of the
# execution cycle. System and site-level shell env vars have been set,
# command line processing finished, etc.  Note that the user's .hadooprc
# has not yet been processed.  This is to allow them to override anything
# that may be set here or potentially a dependency!
#
# function _example_hadoop_init
# {
#   # This example expects a home.  So set a default if not set.
#   EXAMPLE_HOME="${EXAMPLE_HOME:-/usr/example}"
# }
#

#
# The _hadoop_classpath function is called when the shell code is
# establishing the classpath.  This function should use the
# shell hadoop_add_classpath function rather than directly
# manipulating the CLASSPATH variable.  This ensures that the
# CLASSPATH does not have duplicates and provides basic
# sanity checks
#
# function _example_hadoop_classpath
# {
#   # jars that should be near the front
#   hadoop_add_classpath "${EXAMPLE_HOME}/share/pre-jars/*" before
#
#   # jars that should be near the back
#   hadoop_add_classpath "${EXAMPLE_HOME}/share/post-jars/*" after
# }

#
# The _hadoop_nativelib function is called when the shell code is
# buliding the locations for linkable shared libraries. Depending
# upon needs, there are shell function calls that are useful
# to use here:
#
# hadoop_add_javalibpath will push the path onto the command line
# and into the java.library.path system property.  In the majority
# of cases, this should be sufficient, especially if the shared
# library has been linked correctly with $ORIGIN.
#
# hadoop_add_ldlibpath will push the path into the LD_LIBRARY_PATH
# env var.  This should be unnecessary for most code.
#
# function _example_hadoop_nativelib
# {
#   # our library is standalone, so just need the basic path
#   # added. Using after so we are later in the link list
#   hadoop_add_javalibpath "${EXAMPLE_HOME}/lib" after
# }

#
# The _hadoop_finalize function is called to finish up whatever
# extra work needs to be done prior to exec'ing java or some other
# binary. This is where command line properties should get added
# and any last minute work.  This is called prior to Hadoop common
# which means that one can override any parameters that Hadoop
# would normally put here... so be careful!
#
# Useful functions here include hadoop_add_param and for
# Windows compabitility, hadoop_translate_cygwin_path.
#
# function _example_hadoop_finalize
# {
#   # we need a property for our feature
#   hadoop_add_param HADOOP_OPTS Dexample.feature "-Dexample.feature=awesome"
# }
