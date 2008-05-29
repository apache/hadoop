#
# Copyright 2005 The Apache Software Foundation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Autoconf input file
# $Id$

# AC - autoconf
# FB - facebook

#########################################################################
# Section 1:
# DO NOT TOUCH EXCEPT TO CHANGE Product-Name and Rev# IN AC_INIT

AC_PREREQ(2.52)
AC_INIT([fuse_dfs], [0.1.0])
#AC_CONFIG_AUX_DIR([/usr/share/automake-1.9])
# To install locally
AC_CANONICAL_TARGET()
FB_INITIALIZE([localinstall])
AC_PREFIX_DEFAULT([`pwd`])


#case $target in
#*64*intel)
#     OS_ARCH=intel64 ;;
#*64*amd* | *64*unknown*)
#     OS_ARCH=amd64 ;;
#$esac
#AC_SUBST(OS_ARCH)
 DEFS=""
AC_SUBST([DEFS])

############################################################################
# Section 2:
# User Configurable system defaults. Change With CAUTION!

# User can include custom makefile rules. Uncomment and update only <name> in PRODUCT_MK.
# Include where appropriate in any Makefile.am as @PRODUCT_MK@

# Default path to external Facebook components and shared build tools I.e fb303 etc.
# To point to other locations set  environment variable EXTERNAL_PATH.
# DO NOT change default. Changing default value requires changing bootstrap.sh.
FB_WITH_EXTERNAL_PATH([`pwd`])

# Pre-defined macro to set optimized build mode. Configure with --disable-opt option to turn off optimization. Default CXXFLAGS set to '-Wall -O3'. In debug mode  CXXFLAGS is  '-Wall -g'
# FB_ENABLE_DEFAULT_DEBUG_BUILD
FB_ENABLE_DEFAULT_OPT_BUILD

# Predefined macro to set static library mode. Configure with --disable-static option to turn off static lib mode.
# FB_ENABLE_DEFAULT_SHARED
FB_ENABLE_DEFAULT_STATIC

##########################################################################
# Section 3:
# User Configurable

# Personalized FLAG setting macro. Sets FLAG to user specifed value overriding any default.
# $(FLAG) can be used in Makefile.am and global mk's.
# FB_SET_FLAG_VALUE([<FLAGNAME>], [<value>])
# FB_SET_FLAG_VALUE([CXXFLAGS], [-g -O3])

# Personalized feature generator. Creates defines/conditionals and --enable --disable command line options. Doesn't enable until configured with --enable-<feature> option.
# FB_ENABLE_FEATURE([FEATURE], [feature]) OR FB_ENABLE_FEATURE([FEATURE], [feature], [\"<value>\"])
# Example: Macro supplies -DFACEBOOK at compile time and "if FACEBOOK endif" capabilities.

# FB_ENABLE_FEATURE([FACEBOOK], [facebook])
#FB_ENABLE_FEATURE([HDFS], [hdfs])

# Personalized path generator Sets default paths. Provides --with-xx=DIR options.
# FB_WITH_PATH([<var>_home], [<var>path], [<default location>]
# Example: sets $(thrift_dir) variable with default path.
#FB_WITH_PATH([HADOOP_HOME], [hadooppath], [/usr/local/share/hadoop/])
#FB_WITH_PATH([external_home], [externalpath], [${EXTERNAL_PATH}/external])
#FB_WITH_PATH([FUSE_HOME], [fusehome], [/usr/local])
#FB_WITH_PATH([JAVA_HOME], [jdkhome], [/usr/local/jdk])
#FB_WITH_PATH([PROTECTED_PATHS], [protectedpaths], [/,/Trash,/user])

# Set default location of "php-config". User can also use "--with-php-config=<path>" to point to another location.
# Run "./configure --help" to see --with options.
# PHPCONFIGLIBS and PHPCONFIGINCS are generated.
# FB_SET_PHP([PHPCONFIGDIR], [/usr/local/bin])

# Generates Makefile from Makefile.am. Modify when new subdirs are added.
# Change Makefile.am also to add subdirectly.
AC_CONFIG_FILES(Makefile src/Makefile)


############################################################################
# Section 4:
# DO NOT TOUCH.

AC_SUBST(PRODUCT_MK)
AC_OUTPUT

#############################################################################
######### FINISH ############################################################

echo "EXTERNAL_PATH $EXTERNAL_PATH"


#
# NOTES FOR USER
# Short cut to create conditional flags.
#enable_facebook="yes"
#AM_CONDITIONAL([FACEBOOK], [test "$enable_facebook" = yes])
#enable_hdfs="yes"
#AM_CONDITIONAL([HDFS], [test "$enable_hdfs" = yes])

# Enable options with --enable and --disable configurable.
#AC_MSG_CHECKING([whether to enable FACEBOOK])
#FACEBOOK=""
#AC_ARG_ENABLE([facebook],
#  [  --enable-facebook     Enable facebook.],
#  [
#     ENABLE_FACEBOOK=$enableval
#  ],
#  [
#     ENABLE_FACEBOOK="no"
#  ]
#)
#AM_CONDITIONAL([FACEBOOK], [test "$ENABLE_FACEBOOK" = yes])
#AC_MSG_RESULT($ENABLE_FACEBOOK)

