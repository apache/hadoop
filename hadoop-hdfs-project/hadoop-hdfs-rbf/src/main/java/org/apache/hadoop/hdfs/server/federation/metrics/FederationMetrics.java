/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.federation.metrics;

import static org.apache.hadoop.util.Time.now;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;

import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.server.federation.resolver.ActiveNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeContext;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamespaceInfo;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys;
import org.apache.hadoop.hdfs.server.federation.router.Router;
import org.apache.hadoop.hdfs.server.federation.router.RouterRpcServer;
import org.apache.hadoop.hdfs.server.federation.store.MembershipStore;
import org.apache.hadoop.hdfs.server.federation.store.MountTableStore;
import org.apache.hadoop.hdfs.server.federation.store.RouterStore;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreService;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetNamenodeRegistrationsRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetNamenodeRegistrationsResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetNamespaceInfoRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetNamespaceInfoResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetRouterRegistrationsRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetRouterRegistrationsResponse;
import org.apache.hadoop.hdfs.server.federation.store.records.BaseRecord;
import org.apache.hadoop.hdfs.server.federation.store.records.MembershipState;
import org.apache.hadoop.hdfs.server.federation.store.records.MembershipStats;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.hdfs.server.federation.store.records.RouterState;
import org.apache.hadoop.hdfs.server.federation.store.records.StateStoreVersion;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.VersionInfo;
import org.codehaus.jettison.json.JSONObject;
import org.eclipse.jetty.util.ajax.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * Implementation of the Router metrics collector.
 */
public class FederationMetrics implements FederationMBean {

  private static final Logger LOG =
      LoggerFactory.getLogger(FederationMetrics.class);

  /** Format for a date. */
  private static final String DATE_FORMAT = "yyyy/MM/dd HH:mm:ss";

  /** Prevent holding the page from load too long. */
  private final long timeOut;


  /** Router interface. */
  private final Router router;

  /** FederationState JMX bean. */
  private ObjectName beanName;

  /** Resolve the namenode for each namespace. */
  private final ActiveNamenodeResolver namenodeResolver;

  /** State store. */
  private final StateStoreService stateStore;
  /** Membership state store. */
  private MembershipStore membershipStore;
  /** Mount table store. */
  private MountTableStore mountTableStore;
  /** Router state store. */
  private RouterStore routerStore;


  public FederationMetrics(Router router) throws IOException {
    this.router = router;

    try {
      StandardMBean bean = new StandardMBean(this, FederationMBean.class);
      this.beanName = MBeans.register("Router", "FederationState", bean);
      LOG.info("Registered Router MBean: {}", this.beanName);
    } catch (NotCompliantMBeanException e) {
      throw new RuntimeException("Bad Router MBean setup", e);
    }

    // Resolve namenode for each nameservice
    this.namenodeResolver = this.router.getNamenodeResolver();

    // State store interfaces
    this.stateStore = this.router.getStateStore();
    if (this.stateStore == null) {
      LOG.error("State store not available");
    } else {
      this.membershipStore = stateStore.getRegisteredRecordStore(
          MembershipStore.class);
      this.mountTableStore = stateStore.getRegisteredRecordStore(
          MountTableStore.class);
      this.routerStore = stateStore.getRegisteredRecordStore(
          RouterStore.class);
    }

    // Initialize the cache for the DN reports
    Configuration conf = router.getConfig();
    this.timeOut = conf.getTimeDuration(RBFConfigKeys.DN_REPORT_TIME_OUT,
        RBFConfigKeys.DN_REPORT_TIME_OUT_MS_DEFAULT, TimeUnit.MILLISECONDS);

  }

  /**
   * Unregister the JMX beans.
   */
  public void close() {
    if (this.beanName != null) {
      MBeans.unregister(beanName);
    }
  }

  @Override
  public String getNamenodes() {
    final Map<String, Map<String, Object>> info = new LinkedHashMap<>();
    try {
      // Get the values from the store
      GetNamenodeRegistrationsRequest request =
          GetNamenodeRegistrationsRequest.newInstance();
      GetNamenodeRegistrationsResponse response =
          membershipStore.getNamenodeRegistrations(request);

      // Order the namenodes
      final List<MembershipState> namenodes = response.getNamenodeMemberships();
      if (namenodes == null || namenodes.size() == 0) {
        return JSON.toString(info);
      }
      List<MembershipState> namenodesOrder = new ArrayList<>(namenodes);
      Collections.sort(namenodesOrder, MembershipState.NAME_COMPARATOR);

      // Dump namenodes information into JSON
      for (MembershipState namenode : namenodesOrder) {
        Map<String, Object> innerInfo = new HashMap<>();
        Map<String, Object> map = getJson(namenode);
        innerInfo.putAll(map);
        long dateModified = namenode.getDateModified();
        long lastHeartbeat = getSecondsSince(dateModified);
        innerInfo.put("lastHeartbeat", lastHeartbeat);
        MembershipStats stats = namenode.getStats();
        long used = stats.getTotalSpace() - stats.getAvailableSpace();
        innerInfo.put("used", used);
        info.put(namenode.getNamenodeKey(),
            Collections.unmodifiableMap(innerInfo));
      }
    } catch (IOException e) {
      LOG.error("Enable to fetch json representation of namenodes {}",
          e.getMessage());
      return "{}";
    }
    return JSON.toString(info);
  }

  @Override
  public String getNameservices() {
    final Map<String, Map<String, Object>> info = new LinkedHashMap<>();
    try {
      final List<MembershipState> namenodes = getActiveNamenodeRegistrations();
      List<MembershipState> namenodesOrder = new ArrayList<>(namenodes);
      Collections.sort(namenodesOrder, MembershipState.NAME_COMPARATOR);

      // Dump namenodes information into JSON
      for (MembershipState namenode : namenodesOrder) {
        Map<String, Object> innerInfo = new HashMap<>();
        Map<String, Object> map = getJson(namenode);
        innerInfo.putAll(map);
        long dateModified = namenode.getDateModified();
        long lastHeartbeat = getSecondsSince(dateModified);
        innerInfo.put("lastHeartbeat", lastHeartbeat);
        MembershipStats stats = namenode.getStats();
        long used = stats.getTotalSpace() - stats.getAvailableSpace();
        innerInfo.put("used", used);
        info.put(namenode.getNamenodeKey(),
            Collections.unmodifiableMap(innerInfo));
      }
    } catch (IOException e) {
      LOG.error("Cannot retrieve nameservices for JMX: {}", e.getMessage());
      return "{}";
    }
    return JSON.toString(info);
  }

  @Override
  public String getMountTable() {
    final List<Map<String, Object>> info = new LinkedList<>();

    try {
      // Get all the mount points in order
      GetMountTableEntriesRequest request =
          GetMountTableEntriesRequest.newInstance("/");
      GetMountTableEntriesResponse response =
          mountTableStore.getMountTableEntries(request);
      final List<MountTable> mounts = response.getEntries();
      List<MountTable> orderedMounts = new ArrayList<>(mounts);
      Collections.sort(orderedMounts, MountTable.SOURCE_COMPARATOR);

      // Dump mount table entries information into JSON
      for (MountTable entry : orderedMounts) {
        // Sumarize destinations
        Set<String> nameservices = new LinkedHashSet<>();
        Set<String> paths = new LinkedHashSet<>();
        for (RemoteLocation location : entry.getDestinations()) {
          nameservices.add(location.getNameserviceId());
          paths.add(location.getDest());
        }

        Map<String, Object> map = getJson(entry);
        // We add some values with a cleaner format
        map.put("dateCreated", getDateString(entry.getDateCreated()));
        map.put("dateModified", getDateString(entry.getDateModified()));

        Map<String, Object> innerInfo = new HashMap<>();
        innerInfo.putAll(map);
        innerInfo.put("nameserviceId", StringUtils.join(",", nameservices));
        innerInfo.put("path", StringUtils.join(",", paths));
        if (nameservices.size() > 1) {
          innerInfo.put("order", entry.getDestOrder().toString());
        } else {
          innerInfo.put("order", "");
        }
        innerInfo.put("readonly", entry.isReadOnly());
        info.add(Collections.unmodifiableMap(innerInfo));
      }
    } catch (IOException e) {
      LOG.error(
          "Cannot generate JSON of mount table from store: {}", e.getMessage());
      return "[]";
    }
    return JSON.toString(info);
  }

  @Override
  public String getRouters() {
    final Map<String, Map<String, Object>> info = new LinkedHashMap<>();
    try {
      // Get all the routers in order
      GetRouterRegistrationsRequest request =
          GetRouterRegistrationsRequest.newInstance();
      GetRouterRegistrationsResponse response =
          routerStore.getRouterRegistrations(request);
      final List<RouterState> routers = response.getRouters();
      List<RouterState> routersOrder = new ArrayList<>(routers);
      Collections.sort(routersOrder);

      // Dump router information into JSON
      for (RouterState record : routersOrder) {
        Map<String, Object> innerInfo = new HashMap<>();
        Map<String, Object> map = getJson(record);
        innerInfo.putAll(map);
        long dateModified = record.getDateModified();
        long lastHeartbeat = getSecondsSince(dateModified);
        innerInfo.put("lastHeartbeat", lastHeartbeat);

        StateStoreVersion stateStoreVersion = record.getStateStoreVersion();
        if (stateStoreVersion == null) {
          LOG.error("Cannot get State Store versions");
        } else {
          setStateStoreVersions(innerInfo, stateStoreVersion);
        }

        info.put(record.getPrimaryKey(),
            Collections.unmodifiableMap(innerInfo));
      }
    } catch (IOException e) {
      LOG.error("Cannot get Routers JSON from the State Store", e);
      return "{}";
    }
    return JSON.toString(info);
  }

  /**
   * Populate the map with the State Store versions.
   *
   * @param innerInfo Map with the information.
   * @param version State Store versions.
   */
  private static void setStateStoreVersions(
      Map<String, Object> map, StateStoreVersion version) {

    long membershipVersion = version.getMembershipVersion();
    String lastMembershipUpdate = getDateString(membershipVersion);
    map.put("lastMembershipUpdate", lastMembershipUpdate);

    long mountTableVersion = version.getMountTableVersion();
    String lastMountTableDate = getDateString(mountTableVersion);
    map.put("lastMountTableUpdate", lastMountTableDate);
  }

  @Override
  public long getTotalCapacity() {
    return getNameserviceAggregatedLong(MembershipStats::getTotalSpace);
  }

  @Override
  public long getRemainingCapacity() {
    return getNameserviceAggregatedLong(MembershipStats::getAvailableSpace);
  }

  @Override
  public long getProvidedSpace() {
    return getNameserviceAggregatedLong(MembershipStats::getProvidedSpace);
  }

  @Override
  public long getUsedCapacity() {
    return getTotalCapacity() - getRemainingCapacity();
  }

  @Override
  public int getNumNameservices() {
    try {
      Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
      return nss.size();
    } catch (IOException e) {
      LOG.error(
          "Cannot fetch number of expired registrations from the store: {}",
          e.getMessage());
      return 0;
    }
  }

  @Override
  public int getNumNamenodes() {
    try {
      GetNamenodeRegistrationsRequest request =
          GetNamenodeRegistrationsRequest.newInstance();
      GetNamenodeRegistrationsResponse response =
          membershipStore.getNamenodeRegistrations(request);
      List<MembershipState> memberships = response.getNamenodeMemberships();
      return memberships.size();
    } catch (IOException e) {
      LOG.error("Cannot retrieve numNamenodes for JMX: {}", e.getMessage());
      return 0;
    }
  }

  @Override
  public int getNumExpiredNamenodes() {
    try {
      GetNamenodeRegistrationsRequest request =
          GetNamenodeRegistrationsRequest.newInstance();
      GetNamenodeRegistrationsResponse response =
          membershipStore.getExpiredNamenodeRegistrations(request);
      List<MembershipState> expiredMemberships =
          response.getNamenodeMemberships();
      return expiredMemberships.size();
    } catch (IOException e) {
      LOG.error(
          "Cannot retrieve numExpiredNamenodes for JMX: {}", e.getMessage());
      return 0;
    }
  }

  @Override
  public int getNumLiveNodes() {
    return getNameserviceAggregatedInt(
        MembershipStats::getNumOfActiveDatanodes);
  }

  @Override
  public int getNumDeadNodes() {
    return getNameserviceAggregatedInt(MembershipStats::getNumOfDeadDatanodes);
  }

  @Override
  public int getNumDecommissioningNodes() {
    return getNameserviceAggregatedInt(
        MembershipStats::getNumOfDecommissioningDatanodes);
  }

  @Override
  public int getNumDecomLiveNodes() {
    return getNameserviceAggregatedInt(
        MembershipStats::getNumOfDecomActiveDatanodes);
  }

  @Override
  public int getNumDecomDeadNodes() {
    return getNameserviceAggregatedInt(
        MembershipStats::getNumOfDecomDeadDatanodes);
  }

  @Override // NameNodeMXBean
  public String getNodeUsage() {
    float median = 0;
    float max = 0;
    float min = 0;
    float dev = 0;

    final Map<String, Map<String, Object>> info = new HashMap<>();
    try {
      RouterRpcServer rpcServer = this.router.getRpcServer();
      DatanodeInfo[] live = rpcServer.getDatanodeReport(
          DatanodeReportType.LIVE, false, timeOut);

      if (live.length > 0) {
        float totalDfsUsed = 0;
        float[] usages = new float[live.length];
        int i = 0;
        for (DatanodeInfo dn : live) {
          usages[i++] = dn.getDfsUsedPercent();
          totalDfsUsed += dn.getDfsUsedPercent();
        }
        totalDfsUsed /= live.length;
        Arrays.sort(usages);
        median = usages[usages.length / 2];
        max = usages[usages.length - 1];
        min = usages[0];

        for (i = 0; i < usages.length; i++) {
          dev += (usages[i] - totalDfsUsed) * (usages[i] - totalDfsUsed);
        }
        dev = (float) Math.sqrt(dev / usages.length);
      }
    } catch (IOException e) {
      LOG.error("Cannot get the live nodes: {}", e.getMessage());
    }

    final Map<String, Object> innerInfo = new HashMap<>();
    innerInfo.put("min", StringUtils.format("%.2f%%", min));
    innerInfo.put("median", StringUtils.format("%.2f%%", median));
    innerInfo.put("max", StringUtils.format("%.2f%%", max));
    innerInfo.put("stdDev", StringUtils.format("%.2f%%", dev));
    info.put("nodeUsage", innerInfo);

    return JSON.toString(info);
  }

  @Override
  public long getNumBlocks() {
    return getNameserviceAggregatedLong(MembershipStats::getNumOfBlocks);
  }

  @Override
  public long getNumOfMissingBlocks() {
    return getNameserviceAggregatedLong(MembershipStats::getNumOfBlocksMissing);
  }

  @Override
  public long getNumOfBlocksPendingReplication() {
    return getNameserviceAggregatedLong(
        MembershipStats::getNumOfBlocksPendingReplication);
  }

  @Override
  public long getNumOfBlocksUnderReplicated() {
    return getNameserviceAggregatedLong(
        MembershipStats::getNumOfBlocksUnderReplicated);
  }

  @Override
  public long getNumOfBlocksPendingDeletion() {
    return getNameserviceAggregatedLong(
        MembershipStats::getNumOfBlocksPendingDeletion);
  }

  @Override
  public long getNumFiles() {
    return getNameserviceAggregatedLong(MembershipStats::getNumOfFiles);
  }

  @Override
  public String getRouterStarted() {
    long startTime = this.router.getStartTime();
    return new Date(startTime).toString();
  }

  @Override
  public String getVersion() {
    return VersionInfo.getVersion() + ", r" + VersionInfo.getRevision();
  }

  @Override
  public String getCompiledDate() {
    return VersionInfo.getDate();
  }

  @Override
  public String getCompileInfo() {
    return VersionInfo.getDate() + " by " + VersionInfo.getUser() + " from "
        + VersionInfo.getBranch();
  }

  @Override
  public String getHostAndPort() {
    InetSocketAddress address = this.router.getHttpServerAddress();
    if (address != null) {
      try {
        String hostname = InetAddress.getLocalHost().getHostName();
        int port = address.getPort();
        return hostname + ":" + port;
      } catch (UnknownHostException ignored) { }
    }
    return "Unknown";
  }

  @Override
  public String getRouterId() {
    return this.router.getRouterId();
  }

  @Override
  public String getClusterId() {
    try {
      Collection<String> clusterIds =
          getNamespaceInfo(FederationNamespaceInfo::getClusterId);
      return clusterIds.toString();
    } catch (IOException e) {
      LOG.error("Cannot fetch cluster ID metrics: {}", e.getMessage());
      return "";
    }
  }

  @Override
  public String getBlockPoolId() {
    try {
      Collection<String> blockpoolIds =
          getNamespaceInfo(FederationNamespaceInfo::getBlockPoolId);
      return blockpoolIds.toString();
    } catch (IOException e) {
      LOG.error("Cannot fetch block pool ID metrics: {}", e.getMessage());
      return "";
    }
  }

  @Override
  public String getRouterStatus() {
    return "RUNNING";
  }

  /**
   * Build a set of unique values found in all namespaces.
   *
   * @param f Method reference of the appropriate FederationNamespaceInfo
   *          getter function
   * @return Set of unique string values found in all discovered namespaces.
   * @throws IOException if the query could not be executed.
   */
  private Collection<String> getNamespaceInfo(
      Function<FederationNamespaceInfo, String> f) throws IOException {
    GetNamespaceInfoRequest request = GetNamespaceInfoRequest.newInstance();
    GetNamespaceInfoResponse response =
        membershipStore.getNamespaceInfo(request);
    return response.getNamespaceInfo().stream()
      .map(f)
      .collect(Collectors.toSet());
  }

  /**
   * Get the aggregated value for a method for all nameservices.
   * @param f Method reference
   * @return Aggregated integer.
   */
  private int getNameserviceAggregatedInt(ToIntFunction<MembershipStats> f) {
    try {
      return getActiveNamenodeRegistrations().stream()
               .map(MembershipState::getStats)
               .collect(Collectors.summingInt(f));
    } catch (IOException e) {
      LOG.error("Unable to extract metrics: {}", e.getMessage());
      return 0;
    }
  }

  /**
   * Get the aggregated value for a method for all nameservices.
   * @param f Method reference
   * @return Aggregated long.
   */
  private long getNameserviceAggregatedLong(ToLongFunction<MembershipStats> f) {
    try {
      return getActiveNamenodeRegistrations().stream()
               .map(MembershipState::getStats)
               .collect(Collectors.summingLong(f));
    } catch (IOException e) {
      LOG.error("Unable to extract metrics: {}", e.getMessage());
      return 0;
    }
  }

  /**
   * Fetches the most active namenode memberships for all known nameservices.
   * The fetched membership may not or may not be active. Excludes expired
   * memberships.
   * @throws IOException if the query could not be performed.
   * @return List of the most active NNs from each known nameservice.
   */
  private List<MembershipState> getActiveNamenodeRegistrations()
      throws IOException {

    List<MembershipState> resultList = new ArrayList<>();
    GetNamespaceInfoRequest request = GetNamespaceInfoRequest.newInstance();
    GetNamespaceInfoResponse response =
        membershipStore.getNamespaceInfo(request);
    for (FederationNamespaceInfo nsInfo : response.getNamespaceInfo()) {
      // Fetch the most recent namenode registration
      String nsId = nsInfo.getNameserviceId();
      List<? extends FederationNamenodeContext> nns =
          namenodeResolver.getNamenodesForNameserviceId(nsId);
      if (nns != null) {
        FederationNamenodeContext nn = nns.get(0);
        if (nn instanceof MembershipState) {
          resultList.add((MembershipState) nn);
        }
      }
    }
    return resultList;
  }

  /**
   * Get time as a date string.
   * @param time Seconds since 1970.
   * @return String representing the date.
   */
  @VisibleForTesting
  static String getDateString(long time) {
    if (time <= 0) {
      return "-";
    }
    Date date = new Date(time);

    SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);
    return sdf.format(date);
  }

  /**
   * Get the number of seconds passed since a date.
   *
   * @param timeMs to use as a reference.
   * @return Seconds since the date.
   */
  private static long getSecondsSince(long timeMs) {
    if (timeMs < 0) {
      return -1;
    }
    return (now() - timeMs) / 1000;
  }

  /**
   * Get JSON for this record.
   *
   * @return Map representing the data for the JSON representation.
   */
  private static Map<String, Object> getJson(BaseRecord record) {
    Map<String, Object> json = new HashMap<>();
    Map<String, Class<?>> fields = getFields(record);

    for (String fieldName : fields.keySet()) {
      if (!fieldName.equalsIgnoreCase("proto")) {
        try {
          Object value = getField(record, fieldName);
          if (value instanceof BaseRecord) {
            BaseRecord recordField = (BaseRecord) value;
            json.putAll(getJson(recordField));
          } else {
            json.put(fieldName, value == null ? JSONObject.NULL : value);
          }
        } catch (Exception e) {
          throw new IllegalArgumentException(
              "Cannot serialize field " + fieldName + " into JSON");
        }
      }
    }
    return json;
  }

  /**
   * Returns all serializable fields in the object.
   *
   * @return Map with the fields.
   */
  private static Map<String, Class<?>> getFields(BaseRecord record) {
    Map<String, Class<?>> getters = new HashMap<>();
    for (Method m : record.getClass().getDeclaredMethods()) {
      if (m.getName().startsWith("get")) {
        try {
          Class<?> type = m.getReturnType();
          char[] c = m.getName().substring(3).toCharArray();
          c[0] = Character.toLowerCase(c[0]);
          String key = new String(c);
          getters.put(key, type);
        } catch (Exception e) {
          LOG.error("Cannot execute getter {} on {}", m.getName(), record);
        }
      }
    }
    return getters;
  }

  /**
   * Fetches the value for a field name.
   *
   * @param fieldName the legacy name of the field.
   * @return The field data or null if not found.
   */
  private static Object getField(BaseRecord record, String fieldName) {
    Object result = null;
    Method m = locateGetter(record, fieldName);
    if (m != null) {
      try {
        result = m.invoke(record);
      } catch (Exception e) {
        LOG.error("Cannot get field {} on {}", fieldName, record);
      }
    }
    return result;
  }

  /**
   * Finds the appropriate getter for a field name.
   *
   * @param fieldName The legacy name of the field.
   * @return The matching getter or null if not found.
   */
  private static Method locateGetter(BaseRecord record, String fieldName) {
    for (Method m : record.getClass().getMethods()) {
      if (m.getName().equalsIgnoreCase("get" + fieldName)) {
        return m;
      }
    }
    return null;
  }
}
