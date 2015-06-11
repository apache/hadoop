<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

HDFS NFS Gateway
================

* [HDFS NFS Gateway](#HDFS_NFS_Gateway)
    * [Overview](#Overview)
    * [Configuration](#Configuration)
    * [Start and stop NFS gateway service](#Start_and_stop_NFS_gateway_service)
    * [Verify validity of NFS related services](#Verify_validity_of_NFS_related_services)
    * [Mount the export "/"](#Mount_the_export_)
    * [Allow mounts from unprivileged clients](#Allow_mounts_from_unprivileged_clients)
    * [User authentication and mapping](#User_authentication_and_mapping)

Overview
--------

The NFS Gateway supports NFSv3 and allows HDFS to be mounted as part of the client's local file system. Currently NFS Gateway supports and enables the following usage patterns:

* Users can browse the HDFS file system through their local file system
  on NFSv3 client compatible operating systems.
* Users can download files from the the HDFS file system on to their
  local file system.
* Users can upload files from their local file system directly to the
  HDFS file system.
* Users can stream data directly to HDFS through the mount point. File
  append is supported but random write is not supported.

The NFS gateway machine needs the same thing to run an HDFS client like Hadoop JAR files, HADOOP\_CONF directory. The NFS gateway can be on the same host as DataNode, NameNode, or any HDFS client.

Configuration
-------------

The NFS-gateway uses proxy user to proxy all the users accessing the NFS mounts. In non-secure mode, the user running the gateway is the proxy user, while in secure mode the user in Kerberos keytab is the proxy user. Suppose the proxy user is 'nfsserver' and users belonging to the groups 'users-group1' and 'users-group2' use the NFS mounts, then in core-site.xml of the NameNode, the following two properities must be set and only NameNode needs restart after the configuration change (NOTE: replace the string 'nfsserver' with the proxy user name in your cluster):

    <property>
      <name>hadoop.proxyuser.nfsserver.groups</name>
      <value>root,users-group1,users-group2</value>
      <description>
             The 'nfsserver' user is allowed to proxy all members of the 'users-group1' and
             'users-group2' groups. Note that in most cases you will need to include the
             group "root" because the user "root" (which usually belonges to "root" group) will
             generally be the user that initially executes the mount on the NFS client system.
             Set this to '*' to allow nfsserver user to proxy any group.
      </description>
    </property>

    <property>
      <name>hadoop.proxyuser.nfsserver.hosts</name>
      <value>nfs-client-host1.com</value>
      <description>
             This is the host where the nfs gateway is running. Set this to '*' to allow
             requests from any hosts to be proxied.
      </description>
    </property>

The above are the only required configuration for the NFS gateway in non-secure mode. For Kerberized hadoop clusters, the following configurations need to be added to hdfs-site.xml for the gateway (NOTE: replace string "nfsserver" with the proxy user name and ensure the user contained in the keytab is also the same proxy user):

      <property>
        <name>nfs.keytab.file</name>
        <value>/etc/hadoop/conf/nfsserver.keytab</value> <!-- path to the nfs gateway keytab -->
      </property>

      <property>
        <name>nfs.kerberos.principal</name>
        <value>nfsserver/_HOST@YOUR-REALM.COM</value>
      </property>

The rest of the NFS gateway configurations are optional for both secure and non-secure mode.

*   The AIX NFS client has a [few known issues](https://issues.apache.org/jira/browse/HDFS-6549)
    that prevent it from working correctly by default with the HDFS NFS Gateway. If you want to
    be able to access the HDFS NFS Gateway from AIX, you should set the following configuration
    setting to enable work-arounds for these issues:

        <property>
          <name>nfs.aix.compatibility.mode.enabled</name>
          <value>true</value>
        </property>

    Note that regular, non-AIX clients should NOT enable AIX compatibility mode. The work-arounds
    implemented by AIX compatibility mode effectively disable safeguards to ensure that listing
    of directory contents via NFS returns consistent results, and that all data sent to the NFS
    server can be assured to have been committed.

*   HDFS super-user is the user with the same identity as NameNode process itself and
    the super-user can do anything in that permissions checks never fail for the super-user. 
    If the following property is configured, the superuser on NFS client can access any file
    on HDFS. By default, the super user is not configured in the gateway.
    Note that, even the the superuser is configured, "nfs.exports.allowed.hosts" still takes effect. 
    For example, the superuser will not have write access to HDFS files through the gateway if
    the NFS client host is not allowed to have write access in "nfs.exports.allowed.hosts".

        <property>
          <name>nfs.superuser</name>
          <value>the_name_of_hdfs_superuser</value>
        </property>

It's strongly recommended for the users to update a few configuration properties based on their use cases. All the following configuration properties can be added or updated in hdfs-site.xml.

*   If the client mounts the export with access time update allowed, make sure the following
    property is not disabled in the configuration file. Only NameNode needs to restart after
    this property is changed. On some Unix systems, the user can disable access time update
    by mounting the export with "noatime". If the export is mounted with "noatime", the user
    doesn't need to change the following property and thus no need to restart namenode.

        <property>
          <name>dfs.namenode.accesstime.precision</name>
          <value>3600000</value>
          <description>The access time for HDFS file is precise upto this value.
            The default value is 1 hour. Setting a value of 0 disables
            access times for HDFS.
          </description>
        </property>

*   Users are expected to update the file dump directory. NFS client often
    reorders writes, especially when the export is not mounted with "sync" option.
    Sequential writes can arrive at the NFS gateway at random
    order. This directory is used to temporarily save out-of-order writes
    before writing to HDFS. For each file, the out-of-order writes are dumped after
    they are accumulated to exceed certain threshold (e.g., 1MB) in memory.
    One needs to make sure the directory has enough
    space. For example, if the application uploads 10 files with each having
    100MB, it is recommended for this directory to have roughly 1GB space in case if a
    worst-case write reorder happens to every file. Only NFS gateway needs to restart after
    this property is updated.

          <property>
            <name>nfs.dump.dir</name>
            <value>/tmp/.hdfs-nfs</value>
          </property>

*   By default, the export can be mounted by any client. To better control the access,
    users can update the following property. The value string contains machine name and
    access privilege, separated by whitespace
    characters. The machine name format can be a single host, a "*", a Java regular expression, or an IPv4 address. The access
    privilege uses rw or ro to specify read/write or read-only access of the machines to exports. If the access privilege is not provided, the default is read-only. Entries are separated by ";".
    For example: "192.168.0.0/22 rw ; \\\\w\*\\\\.example\\\\.com ; host1.test.org ro;". Only the NFS gateway needs to restart after
    this property is updated. Note that, here Java regular expression is differnt with the regrulation expression used in 
    Linux NFS export table, such as, using "\\\\w\*\\\\.example\\\\.com" instead of "\*.example.com", "192\\\\.168\\\\.0\\\\.(11|22)"
    instead of "192.168.0.[11|22]" and so on.  

        <property>
          <name>nfs.exports.allowed.hosts</name>
          <value>* rw</value>
        </property>

*   HDFS super-user is the user with the same identity as NameNode process itself and
    the super-user can do anything in that permissions checks never fail for the super-user. 
    If the following property is configured, the superuser on NFS client can access any file
    on HDFS. By default, the super user is not configured in the gateway.
    Note that, even the the superuser is configured, "nfs.exports.allowed.hosts" still takes effect. 
    For example, the superuser will not have write access to HDFS files through the gateway if
    the NFS client host is not allowed to have write access in "nfs.exports.allowed.hosts".

        <property>
          <name>nfs.superuser</name>
          <value>the_name_of_hdfs_superuser</value>
        </property>

*   Metrics. Like other HDFS daemons, the gateway exposes runtime metrics. It is available at `http://gateway-ip:50079/jmx` as a JSON document.
    The NFS handler related metrics is exposed under the name "Nfs3Metrics". The latency histograms can be enabled by adding the following
    property to hdfs-site.xml file.

        <property>
          <name>nfs.metrics.percentiles.intervals</name>
          <value>100</value>
          <description>Enable the latency histograms for read, write and
             commit requests. The time unit is 100 seconds in this example.
          </description>
        </property>

*   JVM and log settings. You can export JVM settings (e.g., heap size and GC log) in
    HADOOP\_NFS3\_OPTS. More NFS related settings can be found in hadoop-env.sh.
    To get NFS debug trace, you can edit the log4j.property file
    to add the following. Note, debug trace, especially for ONCRPC, can be very verbose.

    To change logging level:

            log4j.logger.org.apache.hadoop.hdfs.nfs=DEBUG

    To get more details of ONCRPC requests:

            log4j.logger.org.apache.hadoop.oncrpc=DEBUG

Start and stop NFS gateway service
----------------------------------

Three daemons are required to provide NFS service: rpcbind (or portmap), mountd and nfsd. The NFS gateway process has both nfsd and mountd. It shares the HDFS root "/" as the only export. It is recommended to use the portmap included in NFS gateway package. Even though NFS gateway works with portmap/rpcbind provide by most Linux distributions, the package included portmap is needed on some Linux systems such as RHEL 6.2 and SLES 11, the former due to an [rpcbind bug](https://bugzilla.redhat.com/show_bug.cgi?id=731542). More detailed discussions can be found in [HDFS-4763](https://issues.apache.org/jira/browse/HDFS-4763).

1.  Stop nfsv3 and rpcbind/portmap services provided by the platform (commands can be different on various Unix platforms):

        [root]> service nfs stop
        [root]> service rpcbind stop

2.  Start Hadoop's portmap (needs root privileges):

        [root]> $HADOOP_PREFIX/sbin/hadoop-daemon.sh --script $HADOOP_PREFIX/bin/hdfs start portmap

3.  Start mountd and nfsd.

    No root privileges are required for this command. In non-secure mode, the NFS gateway
    should be started by the proxy user mentioned at the beginning of this user guide.
    While in secure mode, any user can start NFS gateway
    as long as the user has read access to the Kerberos keytab defined in "nfs.keytab.file".

        [hdfs]$ $HADOOP_PREFIX/sbin/hadoop-daemon.sh --script $HADOOP_PREFIX/bin/hdfs start nfs3

4.  Stop NFS gateway services.

        [hdfs]$ $HADOOP_PREFIX/sbin/hadoop-daemon.sh --script $HADOOP_PREFIX/bin/hdfs stop nfs3
        [root]> $HADOOP_PREFIX/sbin/hadoop-daemon.sh --script $HADOOP_PREFIX/bin/hdfs stop portmap

Optionally, you can forgo running the Hadoop-provided portmap daemon and instead use the system portmap daemon on all operating systems if you start the NFS Gateway as root. This will allow the HDFS NFS Gateway to work around the aforementioned bug and still register using the system portmap daemon. To do so, just start the NFS gateway daemon as you normally would, but make sure to do so as the "root" user, and also set the "HADOOP\_PRIVILEGED\_NFS\_USER" environment variable to an unprivileged user. In this mode the NFS Gateway will start as root to perform its initial registration with the system portmap, and then will drop privileges back to the user specified by the HADOOP\_PRIVILEGED\_NFS\_USER afterward and for the rest of the duration of the lifetime of the NFS Gateway process. Note that if you choose this route, you should skip steps 1 and 2 above.

Verify validity of NFS related services
---------------------------------------

1.  Execute the following command to verify if all the services are up and running:

        [root]> rpcinfo -p $nfs_server_ip

    You should see output similar to the following:

               program vers proto   port

               100005    1   tcp   4242  mountd

               100005    2   udp   4242  mountd

               100005    2   tcp   4242  mountd

               100000    2   tcp    111  portmapper

               100000    2   udp    111  portmapper

               100005    3   udp   4242  mountd

               100005    1   udp   4242  mountd

               100003    3   tcp   2049  nfs

               100005    3   tcp   4242  mountd

2.  Verify if the HDFS namespace is exported and can be mounted.

        [root]> showmount -e $nfs_server_ip

    You should see output similar to the following:

                Exports list on $nfs_server_ip :

                / (everyone)

Mount the export "/"
--------------------

Currently NFS v3 only uses TCP as the transportation protocol. NLM is not supported so mount option "nolock" is needed. 
Mount option "sync" is strongly recommended since it can minimize or avoid reordered writes, which results in more predictable throughput.
 Not specifying the sync option may cause unreliable behavior when uploading large files.
 It's recommended to use hard mount. This is because, even after the client sends all data to NFS gateway, it may take NFS gateway some extra time to transfer data to HDFS when writes were reorderd by NFS client Kernel.

If soft mount has to be used, the user should give it a relatively long timeout (at least no less than the default timeout on the host) .

The users can mount the HDFS namespace as shown below:

     [root]>mount -t nfs -o vers=3,proto=tcp,nolock,noacl,sync $server:/  $mount_point

Then the users can access HDFS as part of the local file system except that, hard link and random write are not supported yet. To optimize the performance of large file I/O, one can increase the NFS transfer size(rsize and wsize) during mount. By default, NFS gateway supports 1MB as the maximum transfer size. For larger data transfer size, one needs to update "nfs.rtmax" and "nfs.rtmax" in hdfs-site.xml.

Allow mounts from unprivileged clients
--------------------------------------

In environments where root access on client machines is not generally available, some measure of security can be obtained by ensuring that only NFS clients originating from privileged ports can connect to the NFS server. This feature is referred to as "port monitoring." This feature is not enabled by default in the HDFS NFS Gateway, but can be optionally enabled by setting the following config in hdfs-site.xml on the NFS Gateway machine:

    <property>
      <name>nfs.port.monitoring.disabled</name>
      <value>false</value>
    </property>

User authentication and mapping
-------------------------------

NFS gateway in this release uses AUTH\_UNIX style authentication. When the user on NFS client accesses the mount point, NFS client passes the UID to NFS gateway. NFS gateway does a lookup to find user name from the UID, and then passes the username to the HDFS along with the HDFS requests. For example, if the NFS client has current user as "admin", when the user accesses the mounted directory, NFS gateway will access HDFS as user "admin". To access HDFS as the user "hdfs", one needs to switch the current user to "hdfs" on the client system when accessing the mounted directory.

The system administrator must ensure that the user on NFS client host has the same name and UID as that on the NFS gateway host. This is usually not a problem if the same user management system (e.g., LDAP/NIS) is used to create and deploy users on HDFS nodes and NFS client node. In case the user account is created manually on different hosts, one might need to modify UID (e.g., do "usermod -u 123 myusername") on either NFS client or NFS gateway host in order to make it the same on both sides. More technical details of RPC AUTH\_UNIX can be found in [RPC specification](http://tools.ietf.org/html/rfc1057).

Optionally, the system administrator can configure a custom static mapping file in the event one wishes to access the HDFS NFS Gateway from a system with a completely disparate set of UIDs/GIDs. By default this file is located at "/etc/nfs.map", but a custom location can be configured by setting the "static.id.mapping.file" property to the path of the static mapping file. The format of the static mapping file is similar to what is described in the exports(5) manual page, but roughly it is:

    # Mapping for clients accessing the NFS gateway
    uid 10 100 # Map the remote UID 10 the local UID 100
    gid 11 101 # Map the remote GID 11 to the local GID 101
