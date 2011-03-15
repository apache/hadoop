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
# RPM Spec file for HBase version @version@
#

%define name         hbase
%define version      @version@
%define release      @package.release@

# Installation Locations
%define _source      @package.name@
%define _final_name  @final.name@
%define _prefix      @package.prefix@
%define _bin_dir     %{_prefix}/bin
%define _conf_dir    @package.conf.dir@
%define _include_dir %{_prefix}/include
%define _lib_dir     %{_prefix}/lib
%define _lib64_dir   %{_prefix}/lib64
%define _libexec_dir %{_prefix}/libexec
%define _log_dir     @package.log.dir@
%define _man_dir     %{_prefix}/man
%define _pid_dir     @package.pid.dir@
%define _sbin_dir    %{_prefix}/sbin
%define _share_dir   %{_prefix}/share/hbase
%define _src_dir     %{_prefix}/src
%define _var_dir     %{_prefix}/var/lib

# Build time settings
%define _build_dir  @package.build.dir@
%define _final_name @final.name@
%define debug_package %{nil}

Summary: Default HBase configuration templates
License: Apache License, Version 2.0
URL: http://hbase.apache.org/
Vendor: Apache Software Foundation
Group: Development/Libraries
Name: %{name}
Version: %{version}
Release: %{release} 
Source0: %{_source}
Prefix: %{_prefix}
Prefix: %{_conf_dir}
Prefix: %{_log_dir}
Prefix: %{_pid_dir}
Buildroot: %{_build_dir}
Requires: sh-utils, textutils, /usr/sbin/useradd, /usr/sbin/usermod, /sbin/chkconfig, /sbin/service, jdk >= 1.6, hadoop
AutoReqProv: no
Provides: hbase

%description
Installation of this RPM will setup your machine to run in pseudo-distributed mode where each HBase daemon runs in a separate Java process.

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

mkdir -p ${RPM_BUILD_DIR}%{_conf_dir}
mkdir -p ${RPM_BUILD_DIR}%{_bin_dir}
mkdir -p ${RPM_BUILD_DIR}%{_include_dir}
mkdir -p ${RPM_BUILD_DIR}%{_lib_dir}
mkdir -p ${RPM_BUILD_DIR}%{_libexec_dir}
mkdir -p ${RPM_BUILD_DIR}%{_log_dir}
mkdir -p ${RPM_BUILD_DIR}%{_conf_dir}
mkdir -p ${RPM_BUILD_DIR}%{_man_dir}
mkdir -p ${RPM_BUILD_DIR}%{_pid_dir}
mkdir -p ${RPM_BUILD_DIR}%{_sbin_dir}
mkdir -p ${RPM_BUILD_DIR}%{_share_dir}
mkdir -p ${RPM_BUILD_DIR}%{_src_dir}

cp ${RPM_BUILD_DIR}/%{_final_name}/src/packages/update-hbase-env.sh ${RPM_BUILD_DIR}/%{_final_name}/sbin/update-hbase-env.sh
cp ${RPM_BUILD_DIR}/%{_final_name}/src/packages/rpm/init.d/hbase-master ${RPM_BUILD_DIR}/%{_final_name}/sbin/hbase-master
cp ${RPM_BUILD_DIR}/%{_final_name}/src/packages/rpm/init.d/hbase-regionserver ${RPM_BUILD_DIR}/%{_final_name}/sbin/hbase-regionserver
chmod 0755 ${RPM_BUILD_DIR}/%{_final_name}/sbin/*
rm -f ${RPM_BUILD_DIR}/%{_final_name}/lib/hadoop-core*
rm -f ${RPM_BUILD_DIR}/%{_final_name}/lib/zookeeper*
mv -f ${RPM_BUILD_DIR}/%{_final_name}/conf/* ${RPM_BUILD_DIR}%{_conf_dir}
mv -f ${RPM_BUILD_DIR}/%{_final_name}/* ${RPM_BUILD_DIR}%{_share_dir}

rm -rf ${RPM_BUILD_DIR}/%{_final_name}

%preun
${RPM_INSTALL_PREFIX0}/share/hbase/sbin/update-hbase-env.sh \
       --prefix=${RPM_INSTALL_PREFIX0} \
       --bin-dir=${RPM_INSTALL_PREFIX0}/bin \
       --conf-dir=${RPM_INSTALL_PREFIX1} \
       --log-dir=${RPM_INSTALL_PREFIX2} \
       --pid-dir=${RPM_INSTALL_PREFIX3} \
       --uninstall

%pre
getent group hadoop 2>/dev/null >/dev/null || /usr/sbin/groupadd -r hadoop

/usr/sbin/useradd --comment "HBase" --shell /bin/bash -M -r --groups hadoop --home %{_share_dir} hbase 2> /dev/null || :

%post
${RPM_INSTALL_PREFIX0}/share/hbase/sbin/update-hbase-env.sh \
       --prefix=${RPM_INSTALL_PREFIX0} \
       --bin-dir=${RPM_INSTALL_PREFIX0}/bin \
       --conf-dir=${RPM_INSTALL_PREFIX1} \
       --log-dir=${RPM_INSTALL_PREFIX2} \
       --pid-dir=${RPM_INSTALL_PREFIX3}

%files
%defattr(-,root,root)
%{_prefix}
%config %{_conf_dir}
