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

%define name         hbase-conf-pseudo
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
Requires: hbase == %{version}, sh-utils, textutils, /usr/sbin/useradd, /usr/sbin/usermod, /sbin/chkconfig, /sbin/service, jdk >= 1.6
AutoReqProv: no
Provides: hbase-conf-pseudo

%description
Installation of this RPM will setup your machine to run in pseudo-distributed mode where each HBase daemon runs in a separate Java process.

%prep
%setup -n %{_final_name}

%build
if [ -d ${RPM_BUILD_DIR}%{_conf_dir} ]; then
  rm -rf ${RPM_BUILD_DIR}%{_conf_dir}
fi

mkdir -p ${RPM_BUILD_DIR}%{_conf_dir}
mkdir -p ${RPM_BUILD_DIR}%{_share_dir}/src/packages/conf-pseudo
cp -f ${RPM_BUILD_DIR}/%{_final_name}/src/packages/conf-pseudo/hbase-site.xml ${RPM_BUILD_DIR}%{_share_dir}/src/packages/conf-pseudo/hbase-site.xml
rm -rf ${RPM_BUILD_DIR}/%{_final_name}

%preun
/sbin/chkconfig --del hbase-master
/sbin/chkconfig --del hbase-regionserver
/etc/init.d/hbase-master stop 2>/dev/null >/dev/null
/etc/init.d/hbase-regionserver stop 2>/dev/null >/dev/null
exit 0

%post
cp -f ${RPM_INSTALL_PREFIX0}/share/hbase/src/packages/conf-pseudo/*.xml ${RPM_INSTALL_PREFIX1} 2>/dev/null >/dev/null
/etc/init.d/hadoop-namenode start 2>/dev/null >/dev/null
/etc/init.d/hadoop-datanode start 2>/dev/null >/dev/null
su - hdfs -c "hadoop fs -mkdir /hbase" 2>/dev/null >/dev/null
su - hdfs -c "hadoop fs -chown hbase /hbase" 2>/dev/null >/dev/null
/etc/init.d/hbase-master start 2>/dev/null >/dev/null
/etc/init.d/hbase-regionserver start 2>/dev/null >/dev/null
/sbin/chkconfig hbase-master --add
/sbin/chkconfig hbase-regionserver --add
/sbin/chkconfig hbase-master on
/sbin/chkconfig hbase-regionserver on

%files
%defattr(-,root,root)
%config %{_share_dir}/src/packages/conf-pseudo/hbase-site.xml
