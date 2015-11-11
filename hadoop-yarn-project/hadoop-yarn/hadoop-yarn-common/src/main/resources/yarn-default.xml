<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<!--
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
-->

<!-- Do not modify this file directly.  Instead, copy entries that you -->
<!-- wish to modify from this file into yarn-site.xml and change them -->
<!-- there.  If yarn-site.xml does not already exist, create it.      -->

<configuration>

  <!-- IPC Configs -->
  <property>
    <description>Factory to create client IPC classes.</description>
    <name>yarn.ipc.client.factory.class</name>
  </property>

  <property>
    <description>Factory to create server IPC classes.</description>
    <name>yarn.ipc.server.factory.class</name>
  </property>

  <property>
    <description>Factory to create serializeable records.</description>
    <name>yarn.ipc.record.factory.class</name>
  </property>

  <property>
    <description>RPC class implementation</description>
    <name>yarn.ipc.rpc.class</name>
    <value>org.apache.hadoop.yarn.ipc.HadoopYarnProtoRPC</value>
  </property>
  
  <!-- Resource Manager Configs -->
  <property>
    <description>The hostname of the RM.</description>
    <name>yarn.resourcemanager.hostname</name>
    <value>0.0.0.0</value>
  </property>    
  
  <property>
    <description>The address of the applications manager interface in the RM.</description>
    <name>yarn.resourcemanager.address</name>
    <value>${yarn.resourcemanager.hostname}:8032</value>
  </property>

  <property>
    <description>
      The actual address the server will bind to. If this optional address is
      set, the RPC and webapp servers will bind to this address and the port specified in
      yarn.resourcemanager.address and yarn.resourcemanager.webapp.address, respectively. This
      is most useful for making RM listen to all interfaces by setting to 0.0.0.0.
    </description>
    <name>yarn.resourcemanager.bind-host</name>
    <value></value>
  </property>

  <property>
    <description>The number of threads used to handle applications manager requests.</description>
    <name>yarn.resourcemanager.client.thread-count</name>
    <value>50</value>
  </property>

  <property>
    <description>Number of threads used to launch/cleanup AM.</description>
    <name>yarn.resourcemanager.amlauncher.thread-count</name>
    <value>50</value>
  </property>

  <property>
    <description>Retry times to connect with NM.</description>
    <name>yarn.resourcemanager.nodemanager-connect-retries</name>
    <value>10</value>
  </property>

  <property>
    <description>Timeout in milliseconds when YARN dispatcher tries to drain the
      events. Typically, this happens when service is stopping. e.g. RM drains
      the ATS events dispatcher when stopping.
    </description>
    <name>yarn.dispatcher.drain-events.timeout</name>
    <value>300000</value>
  </property>

  <property>
    <description>The expiry interval for application master reporting.</description>
    <name>yarn.am.liveness-monitor.expiry-interval-ms</name>
    <value>600000</value>
  </property>

  <property>
    <description>The Kerberos principal for the resource manager.</description>
    <name>yarn.resourcemanager.principal</name>
  </property>

  <property>
    <description>The address of the scheduler interface.</description>
    <name>yarn.resourcemanager.scheduler.address</name>
    <value>${yarn.resourcemanager.hostname}:8030</value>
  </property>

  <property>
    <description>Number of threads to handle scheduler interface.</description>
    <name>yarn.resourcemanager.scheduler.client.thread-count</name>
    <value>50</value>
  </property>

  <property>
      <description>
        This configures the HTTP endpoint for Yarn Daemons.The following
        values are supported:
        - HTTP_ONLY : Service is provided only on http
        - HTTPS_ONLY : Service is provided only on https
      </description>
      <name>yarn.http.policy</name>
      <value>HTTP_ONLY</value>
  </property>

  <property>
    <description>The http address of the RM web application.</description>
    <name>yarn.resourcemanager.webapp.address</name>
    <value>${yarn.resourcemanager.hostname}:8088</value>
  </property>

  <property>
    <description>The https adddress of the RM web application.</description>
    <name>yarn.resourcemanager.webapp.https.address</name>
    <value>${yarn.resourcemanager.hostname}:8090</value>
  </property>

  <property>
    <name>yarn.resourcemanager.resource-tracker.address</name>
    <value>${yarn.resourcemanager.hostname}:8031</value>
  </property>

  <property>
    <description>Are acls enabled.</description>
    <name>yarn.acl.enable</name>
    <value>false</value>
  </property>

  <property>
    <description>ACL of who can be admin of the YARN cluster.</description>
    <name>yarn.admin.acl</name>
    <value>*</value>
  </property>

  <property>
    <description>The address of the RM admin interface.</description>
    <name>yarn.resourcemanager.admin.address</name>
    <value>${yarn.resourcemanager.hostname}:8033</value>
  </property>

  <property>
    <description>Number of threads used to handle RM admin interface.</description>
    <name>yarn.resourcemanager.admin.client.thread-count</name>
    <value>1</value>
  </property>

  <property>
    <description>Maximum time to wait to establish connection to
    ResourceManager.</description>
    <name>yarn.resourcemanager.connect.max-wait.ms</name>
    <value>900000</value>
  </property>

  <property>
    <description>How often to try connecting to the
    ResourceManager.</description>
    <name>yarn.resourcemanager.connect.retry-interval.ms</name>
    <value>30000</value>
  </property>

  <property>
    <description>The maximum number of application attempts. It's a global
    setting for all application masters. Each application master can specify
    its individual maximum number of application attempts via the API, but the
    individual number cannot be more than the global upper bound. If it is,
    the resourcemanager will override it. The default number is set to 2, to
    allow at least one retry for AM.</description>
    <name>yarn.resourcemanager.am.max-attempts</name>
    <value>2</value>
  </property>

  <property>
    <description>How often to check that containers are still alive. </description>
    <name>yarn.resourcemanager.container.liveness-monitor.interval-ms</name>
    <value>600000</value>
  </property>

  <property>
    <description>The keytab for the resource manager.</description>
    <name>yarn.resourcemanager.keytab</name>
    <value>/etc/krb5.keytab</value>
  </property>

  <property>
    <description>Flag to enable override of the default kerberos authentication
    filter with the RM authentication filter to allow authentication using
    delegation tokens(fallback to kerberos if the tokens are missing). Only
    applicable when the http authentication type is kerberos.</description>
    <name>yarn.resourcemanager.webapp.delegation-token-auth-filter.enabled</name>
    <value>true</value>
  </property>

  <property>
    <description>Flag to enable cross-origin (CORS) support in the RM. This flag
    requires the CORS filter initializer to be added to the filter initializers
    list in core-site.xml.</description>
    <name>yarn.resourcemanager.webapp.cross-origin.enabled</name>
    <value>false</value>
  </property>

  <property>
    <description>How long to wait until a node manager is considered dead.</description>
    <name>yarn.nm.liveness-monitor.expiry-interval-ms</name>
    <value>600000</value>
  </property>

  <property>
    <description>Path to file with nodes to include.</description>
    <name>yarn.resourcemanager.nodes.include-path</name>
    <value></value>
  </property>

  <property>
    <description>Path to file with nodes to exclude.</description>
    <name>yarn.resourcemanager.nodes.exclude-path</name>
    <value></value>
  </property>

  <property>
    <description>Number of threads to handle resource tracker calls.</description>
    <name>yarn.resourcemanager.resource-tracker.client.thread-count</name>
    <value>50</value>
  </property>

  <property>
    <description>The class to use as the resource scheduler.</description>
    <name>yarn.resourcemanager.scheduler.class</name>
    <value>org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler</value>
  </property>

  <property>
    <description>The minimum allocation for every container request at the RM,
    in MBs. Memory requests lower than this will throw a
    InvalidResourceRequestException.</description>
    <name>yarn.scheduler.minimum-allocation-mb</name>
    <value>1024</value>
  </property>

  <property>
    <description>The maximum allocation for every container request at the RM,
    in MBs. Memory requests higher than this will throw a
    InvalidResourceRequestException.</description>
    <name>yarn.scheduler.maximum-allocation-mb</name>
    <value>8192</value>
  </property>

  <property>
    <description>The minimum allocation for every container request at the RM,
    in terms of virtual CPU cores. Requests lower than this will throw a
    InvalidResourceRequestException.</description>
    <name>yarn.scheduler.minimum-allocation-vcores</name>
    <value>1</value>
  </property>

  <property>
    <description>The maximum allocation for every container request at the RM,
    in terms of virtual CPU cores. Requests higher than this will throw a
    InvalidResourceRequestException.</description>
    <name>yarn.scheduler.maximum-allocation-vcores</name>
    <value>32</value>
  </property>

  <property>
    <description>Enable RM to recover state after starting. If true, then 
      yarn.resourcemanager.store.class must be specified. </description>
    <name>yarn.resourcemanager.recovery.enabled</name>
    <value>false</value>
  </property>

  <property>
    <description>Should RM fail fast if it encounters any errors. By defalt, it
      points to ${yarn.fail-fast}. Errors include:
      1) exceptions when state-store write/read operations fails.
    </description>
    <name>yarn.resourcemanager.fail-fast</name>
    <value>${yarn.fail-fast}</value>
  </property>

  <property>
    <description>Should YARN fail fast if it encounters any errors.
      This is a global config for all other components including RM,NM etc.
      If no value is set for component-specific config (e.g yarn.resourcemanager.fail-fast),
      this value will be the default.
    </description>
    <name>yarn.fail-fast</name>
    <value>false</value>
  </property>

  <property>
    <description>Enable RM work preserving recovery. This configuration is private
    to YARN for experimenting the feature.
    </description>
    <name>yarn.resourcemanager.work-preserving-recovery.enabled</name>
    <value>true</value>
  </property>

  <property>
    <description>Set the amount of time RM waits before allocating new
    containers on work-preserving-recovery. Such wait period gives RM a chance
    to settle down resyncing with NMs in the cluster on recovery, before assigning
    new containers to applications.
    </description>
    <name>yarn.resourcemanager.work-preserving-recovery.scheduling-wait-ms</name>
    <value>10000</value>
  </property>

  <property>
    <description>The class to use as the persistent store.

      If org.apache.hadoop.yarn.server.resourcemanager.recovery.ZKRMStateStore
      is used, the store is implicitly fenced; meaning a single ResourceManager
      is able to use the store at any point in time. More details on this
      implicit fencing, along with setting up appropriate ACLs is discussed
      under yarn.resourcemanager.zk-state-store.root-node.acl.
    </description>
    <name>yarn.resourcemanager.store.class</name>
    <value>org.apache.hadoop.yarn.server.resourcemanager.recovery.FileSystemRMStateStore</value>
  </property>

  <property>
    <description>The maximum number of completed applications RM state
    store keeps, less than or equals to ${yarn.resourcemanager.max-completed-applications}.
    By default, it equals to ${yarn.resourcemanager.max-completed-applications}.
    This ensures that the applications kept in the state store are consistent with
    the applications remembered in RM memory.
    Any values larger than ${yarn.resourcemanager.max-completed-applications} will
    be reset to ${yarn.resourcemanager.max-completed-applications}.
    Note that this value impacts the RM recovery performance.Typically,
    a smaller value indicates better performance on RM recovery.
    </description>
    <name>yarn.resourcemanager.state-store.max-completed-applications</name>
    <value>${yarn.resourcemanager.max-completed-applications}</value>
  </property>

  <property>
    <description>Host:Port of the ZooKeeper server to be used by the RM. This
      must be supplied when using the ZooKeeper based implementation of the
      RM state store and/or embedded automatic failover in a HA setting.
    </description>
    <name>yarn.resourcemanager.zk-address</name>
    <!--value>127.0.0.1:2181</value-->
  </property>

  <property>
    <description>Number of times RM tries to connect to ZooKeeper.</description>
    <name>yarn.resourcemanager.zk-num-retries</name>
    <value>1000</value>
  </property>

  <property>
    <description>Retry interval in milliseconds when connecting to ZooKeeper.
      When HA is enabled, the value here is NOT used. It is generated
      automatically from yarn.resourcemanager.zk-timeout-ms and
      yarn.resourcemanager.zk-num-retries.
    </description>
    <name>yarn.resourcemanager.zk-retry-interval-ms</name>
    <value>1000</value>
  </property>

  <property>
    <description>Full path of the ZooKeeper znode where RM state will be
    stored. This must be supplied when using
    org.apache.hadoop.yarn.server.resourcemanager.recovery.ZKRMStateStore
    as the value for yarn.resourcemanager.store.class</description>
    <name>yarn.resourcemanager.zk-state-store.parent-path</name>
    <value>/rmstore</value>
  </property>

  <property>
    <description>ZooKeeper session timeout in milliseconds. Session expiration
    is managed by the ZooKeeper cluster itself, not by the client. This value is
    used by the cluster to determine when the client's session expires.
    Expirations happens when the cluster does not hear from the client within
    the specified session timeout period (i.e. no heartbeat).</description>
    <name>yarn.resourcemanager.zk-timeout-ms</name>
    <value>10000</value>
  </property>

  <property>
    <description>ACL's to be used for ZooKeeper znodes.</description>
    <name>yarn.resourcemanager.zk-acl</name>
    <value>world:anyone:rwcda</value>
  </property>

  <property>
    <description>
      ACLs to be used for the root znode when using ZKRMStateStore in a HA
      scenario for fencing.

      ZKRMStateStore supports implicit fencing to allow a single
      ResourceManager write-access to the store. For fencing, the
      ResourceManagers in the cluster share read-write-admin privileges on the
      root node, but the Active ResourceManager claims exclusive create-delete
      permissions.

      By default, when this property is not set, we use the ACLs from
      yarn.resourcemanager.zk-acl for shared admin access and
      rm-address:random-number for username-based exclusive create-delete
      access.

      This property allows users to set ACLs of their choice instead of using
      the default mechanism. For fencing to work, the ACLs should be
      carefully set differently on each ResourceManger such that all the
      ResourceManagers have shared admin access and the Active ResourceManger
      takes over (exclusively) the create-delete access.
    </description>
    <name>yarn.resourcemanager.zk-state-store.root-node.acl</name>
  </property>

  <property>
    <description>
        Specify the auths to be used for the ACL's specified in both the
        yarn.resourcemanager.zk-acl and
        yarn.resourcemanager.zk-state-store.root-node.acl properties.  This
        takes a comma-separated list of authentication mechanisms, each of the
        form 'scheme:auth' (the same syntax used for the 'addAuth' command in
        the ZK CLI).
    </description>
    <name>yarn.resourcemanager.zk-auth</name>
  </property>

  <property>
    <description>URI pointing to the location of the FileSystem path where
    RM state will be stored. This must be supplied when using
    org.apache.hadoop.yarn.server.resourcemanager.recovery.FileSystemRMStateStore
    as the value for yarn.resourcemanager.store.class</description>
    <name>yarn.resourcemanager.fs.state-store.uri</name>
    <value>${hadoop.tmp.dir}/yarn/system/rmstore</value>
    <!--value>hdfs://localhost:9000/rmstore</value-->
  </property>

  <property>
    <description>hdfs client retry policy specification. hdfs client retry
    is always enabled. Specified in pairs of sleep-time and number-of-retries
    and (t0, n0), (t1, n1), ..., the first n0 retries sleep t0 milliseconds on
    average, the following n1 retries sleep t1 milliseconds on average, and so on.
    </description>
    <name>yarn.resourcemanager.fs.state-store.retry-policy-spec</name>
    <value>2000, 500</value>
  </property>

  <property>
    <description>the number of retries to recover from IOException in
    FileSystemRMStateStore.
    </description>
    <name>yarn.resourcemanager.fs.state-store.num-retries</name>
    <value>0</value>
  </property>

  <property>
    <description>Retry interval in milliseconds in FileSystemRMStateStore.
    </description>
    <name>yarn.resourcemanager.fs.state-store.retry-interval-ms</name>
    <value>1000</value>
  </property>

  <property>
    <description>Local path where the RM state will be stored when using
    org.apache.hadoop.yarn.server.resourcemanager.recovery.LeveldbRMStateStore
    as the value for yarn.resourcemanager.store.class</description>
    <name>yarn.resourcemanager.leveldb-state-store.path</name>
    <value>${hadoop.tmp.dir}/yarn/system/rmstore</value>
  </property>

  <property>
    <description>Enable RM high-availability. When enabled,
      (1) The RM starts in the Standby mode by default, and transitions to
      the Active mode when prompted to.
      (2) The nodes in the RM ensemble are listed in
      yarn.resourcemanager.ha.rm-ids
      (3) The id of each RM either comes from yarn.resourcemanager.ha.id
      if yarn.resourcemanager.ha.id is explicitly specified or can be
      figured out by matching yarn.resourcemanager.address.{id} with local address
      (4) The actual physical addresses come from the configs of the pattern
      - {rpc-config}.{id}</description>
    <name>yarn.resourcemanager.ha.enabled</name>
    <value>false</value>
  </property>

  <property>
    <description>Enable automatic failover.
      By default, it is enabled only when HA is enabled</description>
    <name>yarn.resourcemanager.ha.automatic-failover.enabled</name>
    <value>true</value>
  </property>

  <property>
    <description>Enable embedded automatic failover.
      By default, it is enabled only when HA is enabled.
      The embedded elector relies on the RM state store to handle fencing,
      and is primarily intended to be used in conjunction with ZKRMStateStore.
    </description>
    <name>yarn.resourcemanager.ha.automatic-failover.embedded</name>
    <value>true</value>
  </property>

  <property>
    <description>The base znode path to use for storing leader information,
      when using ZooKeeper based leader election.</description>
    <name>yarn.resourcemanager.ha.automatic-failover.zk-base-path</name>
    <value>/yarn-leader-election</value>
  </property>

  <property>
    <description>Name of the cluster. In a HA setting,
      this is used to ensure the RM participates in leader
      election for this cluster and ensures it does not affect
      other clusters</description>
    <name>yarn.resourcemanager.cluster-id</name>
    <!--value>yarn-cluster</value-->
  </property>

  <property>
    <description>The list of RM nodes in the cluster when HA is
      enabled. See description of yarn.resourcemanager.ha
      .enabled for full details on how this is used.</description>
    <name>yarn.resourcemanager.ha.rm-ids</name>
    <!--value>rm1,rm2</value-->
  </property>

  <property>
    <description>The id (string) of the current RM. When HA is enabled, this
      is an optional config. The id of current RM can be set by explicitly
      specifying yarn.resourcemanager.ha.id or figured out by matching
      yarn.resourcemanager.address.{id} with local address
      See description of yarn.resourcemanager.ha.enabled
      for full details on how this is used.</description>
    <name>yarn.resourcemanager.ha.id</name>
    <!--value>rm1</value-->
  </property>

  <property>
    <description>When HA is enabled, the class to be used by Clients, AMs and
      NMs to failover to the Active RM. It should extend
      org.apache.hadoop.yarn.client.RMFailoverProxyProvider</description>
    <name>yarn.client.failover-proxy-provider</name>
    <value>org.apache.hadoop.yarn.client.ConfiguredRMFailoverProxyProvider</value>
  </property>

  <property>
    <description>When HA is enabled, the max number of times
      FailoverProxyProvider should attempt failover. When set,
      this overrides the yarn.resourcemanager.connect.max-wait.ms. When
      not set, this is inferred from
      yarn.resourcemanager.connect.max-wait.ms.</description>
    <name>yarn.client.failover-max-attempts</name>
    <!--value>15</value-->
  </property>

  <property>
    <description>When HA is enabled, the sleep base (in milliseconds) to be
      used for calculating the exponential delay between failovers. When set,
      this overrides the yarn.resourcemanager.connect.* settings. When
      not set, yarn.resourcemanager.connect.retry-interval.ms is used instead.
    </description>
    <name>yarn.client.failover-sleep-base-ms</name>
    <!--value>500</value-->
  </property>

  <property>
    <description>When HA is enabled, the maximum sleep time (in milliseconds)
      between failovers. When set, this overrides the
      yarn.resourcemanager.connect.* settings. When not set,
      yarn.resourcemanager.connect.retry-interval.ms is used instead.</description>
    <name>yarn.client.failover-sleep-max-ms</name>
    <!--value>15000</value-->
  </property>

  <property>
    <description>When HA is enabled, the number of retries per
      attempt to connect to a ResourceManager. In other words,
      it is the ipc.client.connect.max.retries to be used during
      failover attempts</description>
    <name>yarn.client.failover-retries</name>
    <value>0</value>
  </property>

  <property>
    <description>When HA is enabled, the number of retries per
      attempt to connect to a ResourceManager on socket timeouts. In other
      words, it is the ipc.client.connect.max.retries.on.timeouts to be used
      during failover attempts</description>
    <name>yarn.client.failover-retries-on-socket-timeouts</name>
    <value>0</value>
  </property>

  <property>
    <description>The maximum number of completed applications RM keeps. </description>
    <name>yarn.resourcemanager.max-completed-applications</name>
    <value>10000</value>
  </property>

  <property>
    <description>Interval at which the delayed token removal thread runs</description>
    <name>yarn.resourcemanager.delayed.delegation-token.removal-interval-ms</name>
    <value>30000</value>
  </property>

  <property>
  <description>If true, ResourceManager will have proxy-user privileges.
    Use case: In a secure cluster, YARN requires the user hdfs delegation-tokens to
    do localization and log-aggregation on behalf of the user. If this is set to true,
    ResourceManager is able to request new hdfs delegation tokens on behalf of
    the user. This is needed by long-running-service, because the hdfs tokens
    will eventually expire and YARN requires new valid tokens to do localization
    and log-aggregation. Note that to enable this use case, the corresponding
    HDFS NameNode has to configure ResourceManager as the proxy-user so that
    ResourceManager can itself ask for new tokens on behalf of the user when
    tokens are past their max-life-time.</description>
    <name>yarn.resourcemanager.proxy-user-privileges.enabled</name>
    <value>false</value>
  </property>

  <property>
    <description>Interval for the roll over for the master key used to generate
        application tokens
    </description>
    <name>yarn.resourcemanager.am-rm-tokens.master-key-rolling-interval-secs</name>
    <value>86400</value>
  </property>

  <property>
    <description>Interval for the roll over for the master key used to generate
        container tokens. It is expected to be much greater than
        yarn.nm.liveness-monitor.expiry-interval-ms and
        yarn.resourcemanager.rm.container-allocation.expiry-interval-ms. Otherwise the
        behavior is undefined.
    </description>
    <name>yarn.resourcemanager.container-tokens.master-key-rolling-interval-secs</name>
    <value>86400</value>
  </property>

  <property>
    <description>The heart-beat interval in milliseconds for every NodeManager in the cluster.</description>
    <name>yarn.resourcemanager.nodemanagers.heartbeat-interval-ms</name>
    <value>1000</value>
  </property>

  <property>
    <description>The minimum allowed version of a connecting nodemanager.  The valid values are
      NONE (no version checking), EqualToRM (the nodemanager's version is equal to
      or greater than the RM version), or a Version String.</description>
    <name>yarn.resourcemanager.nodemanager.minimum.version</name>
    <value>NONE</value>
  </property>

  <property>
    <description>Enable a set of periodic monitors (specified in
        yarn.resourcemanager.scheduler.monitor.policies) that affect the
        scheduler.</description>
    <name>yarn.resourcemanager.scheduler.monitor.enable</name>
    <value>false</value>
  </property>

  <property>
    <description>The list of SchedulingEditPolicy classes that interact with
        the scheduler. A particular module may be incompatible with the
        scheduler, other policies, or a configuration of either.</description>
    <name>yarn.resourcemanager.scheduler.monitor.policies</name>
    <value>org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.ProportionalCapacityPreemptionPolicy</value>
  </property>

  <property>
    <description>The class to use as the configuration provider.
    If org.apache.hadoop.yarn.LocalConfigurationProvider is used,
    the local configuration will be loaded.
    If org.apache.hadoop.yarn.FileSystemBasedConfigurationProvider is used,
    the configuration which will be loaded should be uploaded to remote File system first.
    </description>
    <name>yarn.resourcemanager.configuration.provider-class</name>
    <value>org.apache.hadoop.yarn.LocalConfigurationProvider</value>
    <!-- <value>org.apache.hadoop.yarn.FileSystemBasedConfigurationProvider</value> -->
  </property>

  <property>
    <description>The setting that controls whether yarn system metrics is
    published on the timeline server or not by RM.</description>
    <name>yarn.resourcemanager.system-metrics-publisher.enabled</name>
    <value>false</value>
  </property>

  <property>
    <description>Number of worker threads that send the yarn system metrics
    data.</description>
    <name>yarn.resourcemanager.system-metrics-publisher.dispatcher.pool-size</name>
    <value>10</value>
  </property>

  <!-- Node Manager Configs -->
  <property>
    <description>The hostname of the NM.</description>
    <name>yarn.nodemanager.hostname</name>
    <value>0.0.0.0</value>
  </property>
  
  <property>
    <description>The address of the container manager in the NM.</description>
    <name>yarn.nodemanager.address</name>
    <value>${yarn.nodemanager.hostname}:0</value>
  </property>

  <property>
    <description>
      The actual address the server will bind to. If this optional address is
      set, the RPC and webapp servers will bind to this address and the port specified in
      yarn.nodemanager.address and yarn.nodemanager.webapp.address, respectively. This is
      most useful for making NM listen to all interfaces by setting to 0.0.0.0.
    </description>
    <name>yarn.nodemanager.bind-host</name>
    <value></value>
  </property>

  <property>
    <description>Environment variables that should be forwarded from the NodeManager's environment to the container's.</description>
    <name>yarn.nodemanager.admin-env</name>
    <value>MALLOC_ARENA_MAX=$MALLOC_ARENA_MAX</value>
  </property>

  <property>
    <description>Environment variables that containers may override rather than use NodeManager's default.</description>
    <name>yarn.nodemanager.env-whitelist</name>
    <value>JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,HADOOP_YARN_HOME</value>
  </property>

  <property>
    <description>who will execute(launch) the containers.</description>
    <name>yarn.nodemanager.container-executor.class</name>
    <value>org.apache.hadoop.yarn.server.nodemanager.DefaultContainerExecutor</value>
<!--<value>org.apache.hadoop.yarn.server.nodemanager.LinuxContainerExecutor</value>-->
  </property>

  <property>
    <description>Number of threads container manager uses.</description>
    <name>yarn.nodemanager.container-manager.thread-count</name>
    <value>20</value>
  </property>

  <property>
    <description>Number of threads used in cleanup.</description>
    <name>yarn.nodemanager.delete.thread-count</name>
    <value>4</value>
  </property>

  <property>
    <description>
      Number of seconds after an application finishes before the nodemanager's 
      DeletionService will delete the application's localized file directory
      and log directory.
      
      To diagnose Yarn application problems, set this property's value large
      enough (for example, to 600 = 10 minutes) to permit examination of these
      directories. After changing the property's value, you must restart the 
      nodemanager in order for it to have an effect.

      The roots of Yarn applications' work directories is configurable with
      the yarn.nodemanager.local-dirs property (see below), and the roots
      of the Yarn applications' log directories is configurable with the 
      yarn.nodemanager.log-dirs property (see also below).
    </description>
    <name>yarn.nodemanager.delete.debug-delay-sec</name>
    <value>0</value>
  </property>

  <property>
    <description>Keytab for NM.</description>
    <name>yarn.nodemanager.keytab</name>
    <value>/etc/krb5.keytab</value>
  </property>

  <property>
    <description>List of directories to store localized files in. An 
      application's localized file directory will be found in:
      ${yarn.nodemanager.local-dirs}/usercache/${user}/appcache/application_${appid}.
      Individual containers' work directories, called container_${contid}, will
      be subdirectories of this.
   </description>
    <name>yarn.nodemanager.local-dirs</name>
    <value>${hadoop.tmp.dir}/nm-local-dir</value>
  </property>

  <property>
    <description>It limits the maximum number of files which will be localized
      in a single local directory. If the limit is reached then sub-directories
      will be created and new files will be localized in them. If it is set to
      a value less than or equal to 36 [which are sub-directories (0-9 and then
      a-z)] then NodeManager will fail to start. For example; [for public
      cache] if this is configured with a value of 40 ( 4 files +
      36 sub-directories) and the local-dir is "/tmp/local-dir1" then it will
      allow 4 files to be created directly inside "/tmp/local-dir1/filecache".
      For files that are localized further it will create a sub-directory "0"
      inside "/tmp/local-dir1/filecache" and will localize files inside it
      until it becomes full. If a file is removed from a sub-directory that
      is marked full, then that sub-directory will be used back again to
      localize files.
   </description>
    <name>yarn.nodemanager.local-cache.max-files-per-directory</name>
    <value>8192</value>
  </property>

  <property>
    <description>Address where the localizer IPC is.</description>
    <name>yarn.nodemanager.localizer.address</name>
    <value>${yarn.nodemanager.hostname}:8040</value>
  </property>

  <property>
    <description>Interval in between cache cleanups.</description>
    <name>yarn.nodemanager.localizer.cache.cleanup.interval-ms</name>
    <value>600000</value>
  </property>

  <property>
    <description>Target size of localizer cache in MB, per nodemanager. It is
      a target retention size that only includes resources with PUBLIC and 
      PRIVATE visibility and excludes resources with APPLICATION visibility
    </description>
    <name>yarn.nodemanager.localizer.cache.target-size-mb</name>
    <value>10240</value>
  </property>

  <property>
    <description>Number of threads to handle localization requests.</description>
    <name>yarn.nodemanager.localizer.client.thread-count</name>
    <value>5</value>
  </property>

  <property>
    <description>Number of threads to use for localization fetching.</description>
    <name>yarn.nodemanager.localizer.fetch.thread-count</name>
    <value>4</value>
  </property>

  <property>
    <description>
      Where to store container logs. An application's localized log directory 
      will be found in ${yarn.nodemanager.log-dirs}/application_${appid}.
      Individual containers' log directories will be below this, in directories 
      named container_{$contid}. Each container directory will contain the files
      stderr, stdin, and syslog generated by that container.
    </description>
    <name>yarn.nodemanager.log-dirs</name>
    <value>${yarn.log.dir}/userlogs</value>
  </property>

  <property>
    <description>Whether to enable log aggregation. Log aggregation collects
      each container's logs and moves these logs onto a file-system, for e.g.
      HDFS, after the application completes. Users can configure the
      "yarn.nodemanager.remote-app-log-dir" and
      "yarn.nodemanager.remote-app-log-dir-suffix" properties to determine
      where these logs are moved to. Users can access the logs via the
      Application Timeline Server.
    </description>
    <name>yarn.log-aggregation-enable</name>
    <value>false</value>
  </property>

  <property>
    <description>How long to keep aggregation logs before deleting them.  -1 disables. 
    Be careful set this too small and you will spam the name node.</description>
    <name>yarn.log-aggregation.retain-seconds</name>
    <value>-1</value>
  </property> 
  
  <property>
    <description>How long to wait between aggregated log retention checks.
    If set to 0 or a negative value then the value is computed as one-tenth
    of the aggregated log retention time. Be careful set this too small and
    you will spam the name node.</description>
    <name>yarn.log-aggregation.retain-check-interval-seconds</name>
    <value>-1</value>
  </property>

  <property>
    <description>Time in seconds to retain user logs. Only applicable if
    log aggregation is disabled
    </description>
    <name>yarn.nodemanager.log.retain-seconds</name>
    <value>10800</value>
  </property>

  <property>
    <description>Where to aggregate logs to.</description>
    <name>yarn.nodemanager.remote-app-log-dir</name>
    <value>/tmp/logs</value>
  </property>
  <property>
    <description>The remote log dir will be created at 
      {yarn.nodemanager.remote-app-log-dir}/${user}/{thisParam}
    </description>
    <name>yarn.nodemanager.remote-app-log-dir-suffix</name>
    <value>logs</value>
  </property>

  <property>
    <description>Amount of physical memory, in MB, that can be allocated 
    for containers.</description>
    <name>yarn.nodemanager.resource.memory-mb</name>
    <value>8192</value>
  </property>

  <property>
    <description>Whether physical memory limits will be enforced for
    containers.</description>
    <name>yarn.nodemanager.pmem-check-enabled</name>
    <value>true</value>
  </property>

  <property>
    <description>Whether virtual memory limits will be enforced for
    containers.</description>
    <name>yarn.nodemanager.vmem-check-enabled</name>
    <value>true</value>
  </property>

  <property>
    <description>Ratio between virtual memory to physical memory when
    setting memory limits for containers. Container allocations are
    expressed in terms of physical memory, and virtual memory usage
    is allowed to exceed this allocation by this ratio.
    </description>
    <name>yarn.nodemanager.vmem-pmem-ratio</name>
    <value>2.1</value>
  </property>

  <property>
    <description>Number of vcores that can be allocated
    for containers. This is used by the RM scheduler when allocating
    resources for containers. This is not used to limit the number of
    physical cores used by YARN containers.</description>
    <name>yarn.nodemanager.resource.cpu-vcores</name>
    <value>8</value>
  </property>

  <property>
    <description>Percentage of CPU that can be allocated
    for containers. This setting allows users to limit the amount of
    CPU that YARN containers use. Currently functional only
    on Linux using cgroups. The default is to use 100% of CPU.
    </description>
    <name>yarn.nodemanager.resource.percentage-physical-cpu-limit</name>
    <value>100</value>
  </property>

  <property>
    <description>NM Webapp address.</description>
    <name>yarn.nodemanager.webapp.address</name>
    <value>${yarn.nodemanager.hostname}:8042</value>
  </property>

  <property>
    <description>How often to monitor containers.</description>
    <name>yarn.nodemanager.container-monitor.interval-ms</name>
    <value>3000</value>
  </property>

  <property>
    <description>Class that calculates containers current resource utilization.</description>
    <name>yarn.nodemanager.container-monitor.resource-calculator.class</name>
  </property>

  <property>
    <description>Frequency of running node health script.</description>
    <name>yarn.nodemanager.health-checker.interval-ms</name>
    <value>600000</value>
  </property>

  <property>
    <description>Script time out period.</description>
    <name>yarn.nodemanager.health-checker.script.timeout-ms</name>
    <value>1200000</value>
  </property>

  <property>
    <description>The health check script to run.</description>
    <name>yarn.nodemanager.health-checker.script.path</name>
    <value></value>
  </property>

  <property>
    <description>The arguments to pass to the health check script.</description>
    <name>yarn.nodemanager.health-checker.script.opts</name>
    <value></value>
  </property>

  <property>
    <description>Frequency of running disk health checker code.</description>
    <name>yarn.nodemanager.disk-health-checker.interval-ms</name>
    <value>120000</value>
  </property>

  <property>
    <description>The minimum fraction of number of disks to be healthy for the
    nodemanager to launch new containers. This correspond to both
    yarn.nodemanager.local-dirs and yarn.nodemanager.log-dirs. i.e. If there
    are less number of healthy local-dirs (or log-dirs) available, then
    new containers will not be launched on this node.</description>
    <name>yarn.nodemanager.disk-health-checker.min-healthy-disks</name>
    <value>0.25</value>
  </property>

  <property>
    <description>The maximum percentage of disk space utilization allowed after 
    which a disk is marked as bad. Values can range from 0.0 to 100.0. 
    If the value is greater than or equal to 100, the nodemanager will check 
    for full disk. This applies to yarn.nodemanager.local-dirs and
    yarn.nodemanager.log-dirs.</description>
    <name>yarn.nodemanager.disk-health-checker.max-disk-utilization-per-disk-percentage</name>
    <value>90.0</value>
  </property>

  <property>
    <description>The minimum space that must be available on a disk for
    it to be used. This applies to yarn.nodemanager.local-dirs and
    yarn.nodemanager.log-dirs.</description>
    <name>yarn.nodemanager.disk-health-checker.min-free-space-per-disk-mb</name>
    <value>0</value>
  </property>

  <property>
    <description>The path to the Linux container executor.</description>
    <name>yarn.nodemanager.linux-container-executor.path</name>
  </property>

  <property>
    <description>The class which should help the LCE handle resources.</description>
    <name>yarn.nodemanager.linux-container-executor.resources-handler.class</name>
    <value>org.apache.hadoop.yarn.server.nodemanager.util.DefaultLCEResourcesHandler</value>
    <!-- <value>org.apache.hadoop.yarn.server.nodemanager.util.CgroupsLCEResourcesHandler</value> -->
  </property>

  <property>
    <description>The cgroups hierarchy under which to place YARN proccesses (cannot contain commas).
    If yarn.nodemanager.linux-container-executor.cgroups.mount is false (that is, if cgroups have
    been pre-configured), then this cgroups hierarchy must already exist and be writable by the
    NodeManager user, otherwise the NodeManager may fail.
    Only used when the LCE resources handler is set to the CgroupsLCEResourcesHandler.</description>
    <name>yarn.nodemanager.linux-container-executor.cgroups.hierarchy</name>
    <value>/hadoop-yarn</value>
  </property>

  <property>
    <description>Whether the LCE should attempt to mount cgroups if not found.
    Only used when the LCE resources handler is set to the CgroupsLCEResourcesHandler.</description>
    <name>yarn.nodemanager.linux-container-executor.cgroups.mount</name>
    <value>false</value>
  </property>

  <property>
    <description>Where the LCE should attempt to mount cgroups if not found. Common locations
    include /sys/fs/cgroup and /cgroup; the default location can vary depending on the Linux
    distribution in use. This path must exist before the NodeManager is launched.
    Only used when the LCE resources handler is set to the CgroupsLCEResourcesHandler, and
    yarn.nodemanager.linux-container-executor.cgroups.mount is true.</description>
    <name>yarn.nodemanager.linux-container-executor.cgroups.mount-path</name>
  </property>

  <property>
    <description>This determines which of the two modes that LCE should use on
      a non-secure cluster.  If this value is set to true, then all containers
      will be launched as the user specified in
      yarn.nodemanager.linux-container-executor.nonsecure-mode.local-user.  If
      this value is set to false, then containers will run as the user who
      submitted the application.</description>
    <name>yarn.nodemanager.linux-container-executor.nonsecure-mode.limit-users</name>
    <value>true</value>
  </property>

  <property>
    <description>The UNIX user that containers will run as when
      Linux-container-executor is used in nonsecure mode (a use case for this
      is using cgroups) if the
      yarn.nodemanager.linux-container-executor.nonsecure-mode.limit-users is
      set to true.</description>
    <name>yarn.nodemanager.linux-container-executor.nonsecure-mode.local-user</name>
    <value>nobody</value>
  </property>

  <property>
    <description>The allowed pattern for UNIX user names enforced by
    Linux-container-executor when used in nonsecure mode (use case for this
    is using cgroups). The default value is taken from /usr/sbin/adduser</description>
    <name>yarn.nodemanager.linux-container-executor.nonsecure-mode.user-pattern</name>
    <value>^[_.A-Za-z0-9][-@_.A-Za-z0-9]{0,255}?[$]?$</value>
  </property>

  <property>
    <description>This flag determines whether apps should run with strict resource limits
    or be allowed to consume spare resources if they need them. For example, turning the
    flag on will restrict apps to use only their share of CPU, even if the node has spare
    CPU cycles. The default value is false i.e. use available resources. Please note that
    turning this flag on may reduce job throughput on the cluster.</description>
    <name>yarn.nodemanager.linux-container-executor.cgroups.strict-resource-usage</name>
    <value>false</value>
  </property>

  <property>
    <description>This flag determines whether memory limit will be set for the Windows Job
    Object of the containers launched by the default container executor.</description>
    <name>yarn.nodemanager.windows-container.memory-limit.enabled</name>
    <value>false</value>
  </property>

  <property>
    <description>This flag determines whether CPU limit will be set for the Windows Job
    Object of the containers launched by the default container executor.</description>
    <name>yarn.nodemanager.windows-container.cpu-limit.enabled</name>
    <value>false</value>
  </property>

  <property>
    <description>T-file compression types used to compress aggregated logs.</description>
    <name>yarn.nodemanager.log-aggregation.compression-type</name>
    <value>none</value>
  </property>

  <property>
    <description>The kerberos principal for the node manager.</description>
    <name>yarn.nodemanager.principal</name>
    <value></value>
  </property>

  <property>
    <description>A comma separated list of services where service name should only
      contain a-zA-Z0-9_ and can not start with numbers</description>
    <name>yarn.nodemanager.aux-services</name>
    <value></value>
    <!--<value>mapreduce_shuffle</value>-->
  </property>

  <property>
    <description>No. of ms to wait between sending a SIGTERM and SIGKILL to a container</description>
    <name>yarn.nodemanager.sleep-delay-before-sigkill.ms</name>
    <value>250</value>
  </property>

  <property>
    <description>Max time to wait for a process to come up when trying to cleanup a container</description>
    <name>yarn.nodemanager.process-kill-wait.ms</name>
    <value>2000</value>
  </property>

  <property>
    <description>The minimum allowed version of a resourcemanager that a nodemanager will connect to.  
      The valid values are NONE (no version checking), EqualToNM (the resourcemanager's version is 
      equal to or greater than the NM version), or a Version String.</description>
    <name>yarn.nodemanager.resourcemanager.minimum.version</name>
    <value>NONE</value>
  </property>

  <property>
    <description>Max number of threads in NMClientAsync to process container
    management events</description>
    <name>yarn.client.nodemanager-client-async.thread-pool-max-size</name>
    <value>500</value>
  </property>

  <property>
    <description>Max time to wait to establish a connection to NM</description>
    <name>yarn.client.nodemanager-connect.max-wait-ms</name>
    <value>180000</value>
  </property>

  <property>
    <description>Time interval between each attempt to connect to NM</description>
    <name>yarn.client.nodemanager-connect.retry-interval-ms</name>
    <value>10000</value>
  </property>

  <property>
    <description>
      Maximum number of proxy connections to cache for node managers. If set
      to a value greater than zero then the cache is enabled and the NMClient
      and MRAppMaster will cache the specified number of node manager proxies.
      There will be at max one proxy per node manager. Ex. configuring it to a
      value of 5 will make sure that client will at max have 5 proxies cached
      with 5 different node managers. These connections for these proxies will
      be timed out if idle for more than the system wide idle timeout period.
      Note that this could cause issues on large clusters as many connections
      could linger simultaneously and lead to a large number of connection
      threads. The token used for authentication will be used only at
      connection creation time. If a new token is received then the earlier
      connection should be closed in order to use the new token. This and
      (yarn.client.nodemanager-client-async.thread-pool-max-size) are related
      and should be in sync (no need for them to be equal).
      If the value of this property is zero then the connection cache is
      disabled and connections will use a zero idle timeout to prevent too
      many connection threads on large clusters.
    </description>
    <name>yarn.client.max-cached-nodemanagers-proxies</name>
    <value>0</value>
  </property>
  
  <property>
    <description>Enable the node manager to recover after starting</description>
    <name>yarn.nodemanager.recovery.enabled</name>
    <value>false</value>
  </property>

  <property>
    <description>The local filesystem directory in which the node manager will
    store state when recovery is enabled.</description>
    <name>yarn.nodemanager.recovery.dir</name>
    <value>${hadoop.tmp.dir}/yarn-nm-recovery</value>
  </property>

  <property>
    <description>
    The delay time ms to unregister container metrics after completion.
    </description>
    <name>yarn.nodemanager.container-metrics.unregister-delay-ms</name>
    <value>10000</value>
  </property>

  <!--Docker configuration-->

  <property>
    <name>yarn.nodemanager.docker-container-executor.exec-name</name>
    <value>/usr/bin/docker</value>
    <description>
      Name or path to the Docker client.
    </description>
  </property>

  <!--Map Reduce configuration-->
  <property>
    <name>yarn.nodemanager.aux-services.mapreduce_shuffle.class</name>
    <value>org.apache.hadoop.mapred.ShuffleHandler</value>
  </property>

  <property>
    <name>mapreduce.job.jar</name>
    <value/>
  </property>

  <property>
    <name>mapreduce.job.hdfs-servers</name>
    <value>${fs.defaultFS}</value>
  </property>

  <!-- WebAppProxy Configuration-->
  
  <property>
    <description>The kerberos principal for the proxy, if the proxy is not
    running as part of the RM.</description>
    <name>yarn.web-proxy.principal</name>
    <value/>
  </property>
  
  <property>
    <description>Keytab for WebAppProxy, if the proxy is not running as part of 
    the RM.</description>
    <name>yarn.web-proxy.keytab</name>
  </property>
  
  <property>
    <description>The address for the web proxy as HOST:PORT, if this is not
     given then the proxy will run as part of the RM</description>
     <name>yarn.web-proxy.address</name>
     <value/>
  </property>

  <!-- Applications' Configuration-->
  
  <property>
    <description>
      CLASSPATH for YARN applications. A comma-separated list
      of CLASSPATH entries. When this value is empty, the following default
      CLASSPATH for YARN applications would be used. 
      For Linux:
      $HADOOP_CONF_DIR,
      $HADOOP_COMMON_HOME/share/hadoop/common/*,
      $HADOOP_COMMON_HOME/share/hadoop/common/lib/*,
      $HADOOP_HDFS_HOME/share/hadoop/hdfs/*,
      $HADOOP_HDFS_HOME/share/hadoop/hdfs/lib/*,
      $HADOOP_YARN_HOME/share/hadoop/yarn/*,
      $HADOOP_YARN_HOME/share/hadoop/yarn/lib/*
      For Windows:
      %HADOOP_CONF_DIR%,
      %HADOOP_COMMON_HOME%/share/hadoop/common/*,
      %HADOOP_COMMON_HOME%/share/hadoop/common/lib/*,
      %HADOOP_HDFS_HOME%/share/hadoop/hdfs/*,
      %HADOOP_HDFS_HOME%/share/hadoop/hdfs/lib/*,
      %HADOOP_YARN_HOME%/share/hadoop/yarn/*,
      %HADOOP_YARN_HOME%/share/hadoop/yarn/lib/*
    </description>
    <name>yarn.application.classpath</name>
    <value></value>
  </property>

  <!-- Timeline Service's Configuration-->

  <property>
    <description>Indicate to clients whether timeline service is enabled or not.
    If enabled, clients will put entities and events to the timeline server.
    </description>
    <name>yarn.timeline-service.enabled</name>
    <value>false</value>
  </property>

  <property>
    <description>The hostname of the timeline service web application.</description>
    <name>yarn.timeline-service.hostname</name>
    <value>0.0.0.0</value>
  </property>

  <property>
    <description>This is default address for the timeline server to start the
    RPC server.</description>
    <name>yarn.timeline-service.address</name>
    <value>${yarn.timeline-service.hostname}:10200</value>
  </property>

  <property>
    <description>The http address of the timeline service web application.</description>
    <name>yarn.timeline-service.webapp.address</name>
    <value>${yarn.timeline-service.hostname}:8188</value>
  </property>

  <property>
    <description>The https address of the timeline service web application.</description>
    <name>yarn.timeline-service.webapp.https.address</name>
    <value>${yarn.timeline-service.hostname}:8190</value>
  </property>

  <property>
    <description>
      The actual address the server will bind to. If this optional address is
      set, the RPC and webapp servers will bind to this address and the port specified in
      yarn.timeline-service.address and yarn.timeline-service.webapp.address, respectively.
      This is most useful for making the service listen to all interfaces by setting to
      0.0.0.0.
    </description>
    <name>yarn.timeline-service.bind-host</name>
    <value></value>
  </property>

  <property>
    <description>
      Defines the max number of applications could be fetched using REST API or
      application history protocol and shown in timeline server web ui.
    </description>
    <name>yarn.timeline-service.generic-application-history.max-applications</name>
    <value>10000</value>
  </property>

  <property>
    <description>Store class name for timeline store.</description>
    <name>yarn.timeline-service.store-class</name>
    <value>org.apache.hadoop.yarn.server.timeline.LeveldbTimelineStore</value>
  </property>

  <property>
    <description>Enable age off of timeline store data.</description>
    <name>yarn.timeline-service.ttl-enable</name>
    <value>true</value>
  </property>

  <property>
    <description>Time to live for timeline store data in milliseconds.</description>
    <name>yarn.timeline-service.ttl-ms</name>
    <value>604800000</value>
  </property>

  <property>
    <description>Store file name for leveldb timeline store.</description>
    <name>yarn.timeline-service.leveldb-timeline-store.path</name>
    <value>${hadoop.tmp.dir}/yarn/timeline</value>
  </property>

  <property>
    <description>Length of time to wait between deletion cycles of leveldb timeline store in milliseconds.</description>
    <name>yarn.timeline-service.leveldb-timeline-store.ttl-interval-ms</name>
    <value>300000</value>
  </property>

  <property>
    <description>Size of read cache for uncompressed blocks for leveldb timeline store in bytes.</description>
    <name>yarn.timeline-service.leveldb-timeline-store.read-cache-size</name>
    <value>104857600</value>
  </property>

  <property>
    <description>Size of cache for recently read entity start times for leveldb timeline store in number of entities.</description>
    <name>yarn.timeline-service.leveldb-timeline-store.start-time-read-cache-size</name>
    <value>10000</value>
  </property>

  <property>
    <description>Size of cache for recently written entity start times for leveldb timeline store in number of entities.</description>
    <name>yarn.timeline-service.leveldb-timeline-store.start-time-write-cache-size</name>
    <value>10000</value>
  </property>

  <property>
    <description>Handler thread count to serve the client RPC requests.</description>
    <name>yarn.timeline-service.handler-thread-count</name>
    <value>10</value>
  </property>

  <property>
    <name>yarn.timeline-service.http-authentication.type</name>
    <value>simple</value>
    <description>
      Defines authentication used for the timeline server HTTP endpoint.
      Supported values are: simple | kerberos | #AUTHENTICATION_HANDLER_CLASSNAME#
    </description>
  </property>

  <property>
    <name>yarn.timeline-service.http-authentication.simple.anonymous.allowed</name>
    <value>true</value>
    <description>
      Indicates if anonymous requests are allowed by the timeline server when using
      'simple' authentication.
    </description>
  </property>

  <property>
    <description>The Kerberos principal for the timeline server.</description>
    <name>yarn.timeline-service.principal</name>
    <value></value>
  </property>

  <property>
    <description>The Kerberos keytab for the timeline server.</description>
    <name>yarn.timeline-service.keytab</name>
    <value>/etc/krb5.keytab</value>
  </property>

  <property>
    <description>Comma separated list of UIs that will be hosted</description>
    <name>yarn.timeline-service.ui-names</name>
    <value></value>
  </property>

  <property>
    <description>
    Default maximum number of retires for timeline servive client
    and value -1 means no limit.
    </description>
    <name>yarn.timeline-service.client.max-retries</name>
    <value>30</value>
  </property>

  <property>
    <description>Client policy for whether timeline operations are non-fatal</description>
    <name>yarn.timeline-service.client.best-effort</name>
    <value>false</value>
  </property>

  <property>
    <description>
    Default retry time interval for timeline servive client.
    </description>
    <name>yarn.timeline-service.client.retry-interval-ms</name>
    <value>1000</value>
  </property>

  <property>
    <description>Enable timeline server to recover state after starting. If
    true, then yarn.timeline-service.state-store-class must be specified.
    </description>
    <name>yarn.timeline-service.recovery.enabled</name>
    <value>false</value>
  </property>

  <property>
    <description>Store class name for timeline state store.</description>
    <name>yarn.timeline-service.state-store-class</name>
    <value>org.apache.hadoop.yarn.server.timeline.recovery.LeveldbTimelineStateStore</value>
  </property>

  <property>
    <description>Store file name for leveldb state store.</description>
    <name>yarn.timeline-service.leveldb-state-store.path</name>
    <value>${hadoop.tmp.dir}/yarn/timeline</value>
  </property>

  <!--  Shared Cache Configuration -->
  <property>
    <description>Whether the shared cache is enabled</description>
    <name>yarn.sharedcache.enabled</name>
    <value>false</value>
  </property>

  <property>
    <description>The root directory for the shared cache</description>
    <name>yarn.sharedcache.root-dir</name>
    <value>/sharedcache</value>
  </property>

  <property>
    <description>The level of nested directories before getting to the checksum
    directories. It must be non-negative.</description>
    <name>yarn.sharedcache.nested-level</name>
    <value>3</value>
  </property>

  <property>
    <description>The implementation to be used for the SCM store</description>
    <name>yarn.sharedcache.store.class</name>
    <value>org.apache.hadoop.yarn.server.sharedcachemanager.store.InMemorySCMStore</value>
  </property>

  <property>
    <description>The implementation to be used for the SCM app-checker</description>
    <name>yarn.sharedcache.app-checker.class</name>
    <value>org.apache.hadoop.yarn.server.sharedcachemanager.RemoteAppChecker</value>
  </property>
  
  <property>
    <description>A resource in the in-memory store is considered stale
    if the time since the last reference exceeds the staleness period.
    This value is specified in minutes.</description>
    <name>yarn.sharedcache.store.in-memory.staleness-period-mins</name>
    <value>10080</value>
  </property>
  
  <property>
    <description>Initial delay before the in-memory store runs its first check
    to remove dead initial applications. Specified in minutes.</description>
    <name>yarn.sharedcache.store.in-memory.initial-delay-mins</name>
    <value>10</value>
  </property>
  
  <property>
    <description>The frequency at which the in-memory store checks to remove
    dead initial applications. Specified in minutes.</description>
    <name>yarn.sharedcache.store.in-memory.check-period-mins</name>
    <value>720</value>
  </property>
  
  <property>
    <description>The address of the admin interface in the SCM (shared cache manager)</description>
    <name>yarn.sharedcache.admin.address</name>
    <value>0.0.0.0:8047</value>
  </property>

  <property>
    <description>The number of threads used to handle SCM admin interface (1 by default)</description>
    <name>yarn.sharedcache.admin.thread-count</name>
    <value>1</value>
  </property>

  <property>
    <description>The address of the web application in the SCM (shared cache manager)</description>
    <name>yarn.sharedcache.webapp.address</name>
    <value>0.0.0.0:8788</value>
  </property>

  <property>
    <description>The frequency at which a cleaner task runs.
    Specified in minutes.</description>
    <name>yarn.sharedcache.cleaner.period-mins</name>
    <value>1440</value>
  </property>

  <property>
    <description>Initial delay before the first cleaner task is scheduled.
    Specified in minutes.</description>
    <name>yarn.sharedcache.cleaner.initial-delay-mins</name>
    <value>10</value>
  </property>

  <property>
    <description>The time to sleep between processing each shared cache
    resource. Specified in milliseconds.</description>
    <name>yarn.sharedcache.cleaner.resource-sleep-ms</name>
    <value>0</value>
  </property>

  <property>
    <description>The address of the node manager interface in the SCM
    (shared cache manager)</description>
    <name>yarn.sharedcache.uploader.server.address</name>
    <value>0.0.0.0:8046</value>
  </property>

  <property>
    <description>The number of threads used to handle shared cache manager
    requests from the node manager (50 by default)</description>
    <name>yarn.sharedcache.uploader.server.thread-count</name>
    <value>50</value>
  </property>

  <property>
    <description>The address of the client interface in the SCM
    (shared cache manager)</description>
    <name>yarn.sharedcache.client-server.address</name>
    <value>0.0.0.0:8045</value>
  </property>

  <property>
    <description>The number of threads used to handle shared cache manager
    requests from clients (50 by default)</description>
    <name>yarn.sharedcache.client-server.thread-count</name>
    <value>50</value>
  </property>

  <property>
    <description>The algorithm used to compute checksums of files (SHA-256 by
    default)</description>
    <name>yarn.sharedcache.checksum.algo.impl</name>
    <value>org.apache.hadoop.yarn.sharedcache.ChecksumSHA256Impl</value>
  </property>

  <property>
    <description>The replication factor for the node manager uploader for the
    shared cache (10 by default)</description>
    <name>yarn.sharedcache.nm.uploader.replication.factor</name>
    <value>10</value>
  </property>

  <property>
    <description>The number of threads used to upload files from a node manager
    instance (20 by default)</description>
    <name>yarn.sharedcache.nm.uploader.thread-count</name>
    <value>20</value>
  </property>

  <!-- Other configuration -->
  <property>
    <description>The interval that the yarn client library uses to poll the
    completion status of the asynchronous API of application client protocol.
    </description>
    <name>yarn.client.application-client-protocol.poll-interval-ms</name>
    <value>200</value>
  </property>

  <property>
    <description>RSS usage of a process computed via 
    /proc/pid/stat is not very accurate as it includes shared pages of a
    process. /proc/pid/smaps provides useful information like
    Private_Dirty, Private_Clean, Shared_Dirty, Shared_Clean which can be used
    for computing more accurate RSS. When this flag is enabled, RSS is computed
    as Min(Shared_Dirty, Pss) + Private_Clean + Private_Dirty. It excludes
    read-only shared mappings in RSS computation.  
    </description>
    <name>yarn.nodemanager.container-monitor.procfs-tree.smaps-based-rss.enabled</name>
    <value>false</value>
  </property>

  <property>
    <description>Defines how often NMs wake up to upload log files.
    The default value is -1. By default, the logs will be uploaded when
    the application is finished. By setting this configure, logs can be uploaded
    periodically when the application is running. The minimum rolling-interval-seconds
    can be set is 3600.
    </description>
    <name>yarn.nodemanager.log-aggregation.roll-monitoring-interval-seconds</name>
    <value>-1</value>
  </property>

  <property>
    <description>Flag to enable cross-origin (CORS) support in the NM. This flag
    requires the CORS filter initializer to be added to the filter initializers
    list in core-site.xml.</description>
    <name>yarn.nodemanager.webapp.cross-origin.enabled</name>
    <value>false</value>
  </property>
</configuration>
