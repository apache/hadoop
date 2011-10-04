#   Licensed to the Apache Software Foundation (ASF) under one or more
#   contributor license agreements.  See the NOTICE file distributed with
#   this work for additional information regarding copyright ownership.
#   The ASF licenses this file to You under the Apache License, Version 2.0
#   (the "License"); you may not use this file except in compliance with
#   the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

#
# RPM Spec file for Hadoop version @version@
#

%define name          hadoop-hdfs
%define version       @version@
%define release       @package.release@
%define major_version %(echo %{version} | cut -d. -f -2)

# Installation Locations
%define _prefix      @package.prefix@
%define _bin_dir     %{_prefix}/bin
%define _conf_dir    @package.conf.dir@
%define _lib_dir     %{_prefix}/lib
%define _lib64_dir   %{_prefix}/lib64
%define _libexec_dir %{_prefix}/libexec
%define _log_dir     @package.log.dir@
%define _pid_dir     @package.pid.dir@
%define _sbin_dir    %{_prefix}/sbin
%define _share_dir   %{_prefix}/share
%define _var_dir     @package.var.dir@

# Build time settings
%define _build_dir  @package.build.dir@
%define _final_name @final.name@
%define debug_package %{nil}

# Disable brp-java-repack-jars for aspect J
%define __os_install_post    \
    /usr/lib/rpm/redhat/brp-compress \
    %{!?__debug_package:/usr/lib/rpm/redhat/brp-strip %{__strip}} \
    /usr/lib/rpm/redhat/brp-strip-static-archive %{__strip} \
    /usr/lib/rpm/redhat/brp-strip-comment-note %{__strip} %{__objdump} \
    /usr/lib/rpm/brp-python-bytecompile %{nil}

# RPM searches perl files for dependancies and this breaks for non packaged perl lib
# like thrift so disable this
%define _use_internal_dependency_generator 0

%ifarch i386
%global hadoop_arch Linux-i386-32
%endif
%ifarch amd64 x86_64
%global hadoop_arch Linux-amd64-64
%endif
%ifarch noarch
%global hadoop_arch ""
%endif

Summary: The Apache Hadoop project develops open-source software for reliable, scalable, distributed computing
License: Apache License, Version 2.0
URL: http://hadoop.apache.org/core/
Vendor: Apache Software Foundation
Group: Development/Libraries
Name: %{name}
Version: %{version}
Release: %{release} 
Source0: %{_final_name}-bin.tar.gz
Prefix: %{_prefix}
Prefix: %{_conf_dir}
Prefix: %{_log_dir}
Prefix: %{_pid_dir}
Buildroot: %{_build_dir}
Requires: sh-utils, textutils, /usr/sbin/useradd, /usr/sbin/usermod, /sbin/chkconfig, /sbin/service, hadoop-common >= %{major_version}.0, hadoop-common <= %{major_version}.9999
AutoReqProv: no
Provides: hadoop-hdfs

%description
The Apache Hadoop project develops open-source software for reliable, scalable, 
distributed computing.  Hadoop includes these subprojects:

HDFS: A distributed file system that provides high throughput access to application data.

%prep
%setup -n %{_final_name}

%build
if [ -d ${RPM_BUILD_DIR}%{_prefix} ]; then
  rm -rf ${RPM_BUILD_DIR}%{_prefix}
fi

if [ -d ${RPM_BUILD_DIR}%{_log_dir} ]; then
  rm -rf ${RPM_BUILD_DIR}%{_log_dir}
fi

if [ -d ${RPM_BUILD_DIR}%{_conf_dir} ]; then
  rm -rf ${RPM_BUILD_DIR}%{_conf_dir}
fi

if [ -d ${RPM_BUILD_DIR}%{_pid_dir} ]; then
  rm -rf ${RPM_BUILD_DIR}%{_pid_dir}
fi

mkdir -p ${RPM_BUILD_DIR}%{_prefix}
mkdir -p ${RPM_BUILD_DIR}%{_bin_dir}
mkdir -p ${RPM_BUILD_DIR}%{_lib_dir}
%ifarch amd64 x86_64
mkdir -p ${RPM_BUILD_DIR}%{_lib64_dir}
%endif
mkdir -p ${RPM_BUILD_DIR}%{_libexec_dir}
mkdir -p ${RPM_BUILD_DIR}%{_log_dir}
mkdir -p ${RPM_BUILD_DIR}%{_conf_dir}
mkdir -p ${RPM_BUILD_DIR}%{_pid_dir}
mkdir -p ${RPM_BUILD_DIR}%{_sbin_dir}
mkdir -p ${RPM_BUILD_DIR}%{_share_dir}
mkdir -p ${RPM_BUILD_DIR}%{_var_dir}
mkdir -p ${RPM_BUILD_DIR}/etc/init.d

cp ${RPM_BUILD_DIR}/%{_final_name}/sbin/hadoop-namenode.redhat ${RPM_BUILD_DIR}/etc/init.d/hadoop-namenode
cp ${RPM_BUILD_DIR}/%{_final_name}/sbin/hadoop-datanode.redhat ${RPM_BUILD_DIR}/etc/init.d/hadoop-datanode
rm -f ${RPM_BUILD_DIR}/%{_final_name}/sbin/hadoop-namenode.*
rm -f ${RPM_BUILD_DIR}/%{_final_name}/sbin/hadoop-namenode.*

chmod 0755 ${RPM_BUILD_DIR}/etc/init.d/hadoop-*


#########################
#### INSTALL SECTION ####
#########################
%install
mv ${RPM_BUILD_DIR}/%{_final_name}/bin/* ${RPM_BUILD_DIR}%{_bin_dir}
rm ${RPM_BUILD_DIR}/%{_final_name}/etc/hadoop/configuration.xsl
rm ${RPM_BUILD_DIR}/%{_final_name}/etc/hadoop/hadoop-metrics2.properties
mv ${RPM_BUILD_DIR}/%{_final_name}/etc/hadoop/* ${RPM_BUILD_DIR}%{_conf_dir}
mv ${RPM_BUILD_DIR}/%{_final_name}/lib/* ${RPM_BUILD_DIR}%{_lib_dir}
mv ${RPM_BUILD_DIR}/%{_final_name}/libexec/* ${RPM_BUILD_DIR}%{_libexec_dir}
mv ${RPM_BUILD_DIR}/%{_final_name}/sbin/* ${RPM_BUILD_DIR}%{_sbin_dir}
mv ${RPM_BUILD_DIR}/%{_final_name}/share/* ${RPM_BUILD_DIR}%{_share_dir}
rm -rf ${RPM_BUILD_DIR}/%{_final_name}/etc

%pre
getent group hadoop 2>/dev/null >/dev/null || /usr/sbin/groupadd -g 123 -r hadoop
/usr/sbin/useradd --comment "Hadoop HDFS" -u 201 --shell /bin/bash -M -r --groups hadoop --home %{_var_dir}/hdfs hdfs 2> /dev/null || :

%post
bash ${RPM_INSTALL_PREFIX0}/sbin/update-hdfs-env.sh \
       --prefix=${RPM_INSTALL_PREFIX0} \
       --bin-dir=${RPM_INSTALL_PREFIX0}/bin \
       --sbin-dir=${RPM_INSTALL_PREFIX0}/sbin \
       --conf-dir=${RPM_INSTALL_PREFIX1} \
       --log-dir=${RPM_INSTALL_PREFIX2} \
       --pid-dir=${RPM_INSTALL_PREFIX3}

%preun
bash ${RPM_INSTALL_PREFIX0}/sbin/update-hdfs-env.sh \
       --prefix=${RPM_INSTALL_PREFIX0} \
       --bin-dir=${RPM_INSTALL_PREFIX0}/bin \
       --sbin-dir=${RPM_INSTALL_PREFIX0}/sbin \
       --conf-dir=${RPM_INSTALL_PREFIX1} \
       --log-dir=${RPM_INSTALL_PREFIX2} \
       --pid-dir=${RPM_INSTALL_PREFIX3} \
       --uninstall

%files 
%defattr(-,root,root)
%attr(0755,root,hadoop) %{_log_dir}
%attr(0775,root,hadoop) %{_pid_dir}
%config(noreplace) %{_conf_dir}/hdfs-site.xml
%{_prefix}
%attr(0775,root,root) /etc/init.d/hadoop-namenode
%attr(0775,root,root) /etc/init.d/hadoop-datanode
