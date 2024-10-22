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
package org.apache.hadoop.hdfs.server.federation;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.resolver.ActiveNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeContext;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeServiceState;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamespaceInfo;
import org.apache.hadoop.hdfs.server.federation.resolver.FileSubclusterResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.NamenodePriorityComparator;
import org.apache.hadoop.hdfs.server.federation.resolver.NamenodeStatusReport;
import org.apache.hadoop.hdfs.server.federation.resolver.PathLocation;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.hdfs.server.federation.router.Router;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreService;
import org.apache.hadoop.util.Time;

/**
 * In-memory cache/mock of a namenode and file resolver. Stores the most
 * recently updated NN information for each nameservice and block pool. It also
 * stores a virtual mount table for resolving global namespace paths to local NN
 * paths.
 */
public class MockResolver
    implements ActiveNamenodeResolver, FileSubclusterResolver {

  private Map<String, List<? extends FederationNamenodeContext>> resolver =
      new HashMap<>();
  private Map<String, List<RemoteLocation>> locations = new HashMap<>();
  private Set<FederationNamespaceInfo> namespaces = new HashSet<>();
  private String defaultNamespace = null;
  private boolean disableDefaultNamespace = false;
  private volatile boolean disableRegistration = false;
  private TreeSet<String> disableNamespaces = new TreeSet<>();

  public MockResolver() {
    this.cleanRegistrations();
  }

  public MockResolver(Configuration conf) {
    this();
  }

  public MockResolver(Configuration conf, StateStoreService store) {
    this();
  }

  public MockResolver(Configuration conf, Router router) {
    this();
  }

  public void addLocation(String mount, String nsId, String location) {
    List<RemoteLocation> locationsList = this.locations.get(mount);
    if (locationsList == null) {
      locationsList = new LinkedList<>();
      this.locations.put(mount, locationsList);
    }

    final RemoteLocation remoteLocation =
        new RemoteLocation(nsId, location, mount);
    if (!locationsList.contains(remoteLocation)) {
      locationsList.add(remoteLocation);
    }

    if (this.defaultNamespace == null) {
      this.defaultNamespace = nsId;
    }
  }

  public boolean removeLocation(String mount, String nsId, String location) {
    List<RemoteLocation> locationsList = this.locations.get(mount);
    final RemoteLocation remoteLocation =
        new RemoteLocation(nsId, location, mount);
    if (locationsList != null) {
      return locationsList.remove(remoteLocation);
    }
    return false;
  }

  public synchronized void cleanRegistrations() {
    this.resolver = new HashMap<>();
    this.namespaces = new HashSet<>();
  }

  /*
   * Disable NameNode auto registration for test. This method usually used after
   * {@link MockResolver#cleanRegistrations()}, and before {@link
   * MockResolver#registerNamenode()}
   */
  public void setDisableRegistration(boolean isDisable) {
    disableRegistration = isDisable;
  }

  @Override public void updateUnavailableNamenode(String ns,
      InetSocketAddress failedAddress) throws IOException {
    updateNameNodeState(ns, failedAddress,
        FederationNamenodeServiceState.UNAVAILABLE);
  }

  @Override
  public void updateActiveNamenode(
      String nsId, InetSocketAddress successfulAddress) {
    updateNameNodeState(nsId, successfulAddress,
        FederationNamenodeServiceState.ACTIVE);
  }

  private void updateNameNodeState(String nsId,
      InetSocketAddress iAddr,
      FederationNamenodeServiceState state) {
    String sAddress = iAddr.getHostName() + ":" +
        iAddr.getPort();
    String key = nsId;
    if (key != null) {
      // Update the active entry
      @SuppressWarnings("unchecked")
      List<FederationNamenodeContext> namenodes =
          (List<FederationNamenodeContext>) this.resolver.get(key);
      for (FederationNamenodeContext namenode : namenodes) {
        if (namenode.getRpcAddress().equals(sAddress)) {
          MockNamenodeContext nn = (MockNamenodeContext) namenode;
          nn.setState(state);
          break;
        }
      }
      // This operation modifies the list, so we need to be careful
      synchronized(namenodes) {
        Collections.sort(namenodes, new NamenodePriorityComparator());
      }
    }
  }

  @Override
  public synchronized List<? extends FederationNamenodeContext>
      getNamenodesForNameserviceId(String nameserviceId, boolean observerRead) {
    // Return a copy of the list because it is updated periodically
    List<? extends FederationNamenodeContext> namenodes =
        this.resolver.get(nameserviceId);
    if (namenodes == null) {
      namenodes = new ArrayList<>();
    }

    List<FederationNamenodeContext> ret = new ArrayList<>();

    if (observerRead) {
      Iterator<? extends FederationNamenodeContext> iterator = namenodes
          .iterator();
      List<FederationNamenodeContext> observerNN = new ArrayList<>();
      List<FederationNamenodeContext> nonObserverNN = new ArrayList<>();
      while (iterator.hasNext()) {
        FederationNamenodeContext membership = iterator.next();
        if (membership.getState() == FederationNamenodeServiceState.OBSERVER) {
          observerNN.add(membership);
        } else {
          nonObserverNN.add(membership);
        }
      }
      Collections.shuffle(observerNN);
      Collections.sort(nonObserverNN, new NamenodePriorityComparator());
      ret.addAll(observerNN);
      ret.addAll(nonObserverNN);
    } else {
      ret.addAll(namenodes);
      Collections.sort(ret, new NamenodePriorityComparator());
    }

    return Collections.unmodifiableList(ret);
  }

  @Override
  public synchronized List<? extends FederationNamenodeContext>
      getNamenodesForBlockPoolId(String blockPoolId) {
    // Return a copy of the list because it is updated periodically
    List<? extends FederationNamenodeContext> namenodes =
        this.resolver.get(blockPoolId);
    return Collections.unmodifiableList(new ArrayList<>(namenodes));
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  private static class MockNamenodeContext
      implements FederationNamenodeContext {

    private String namenodeId;
    private String nameserviceId;

    private String webScheme;
    private String webAddress;
    private String rpcAddress;
    private String serviceAddress;
    private String lifelineAddress;

    private FederationNamenodeServiceState state;
    private long dateModified;

    MockNamenodeContext(
        String rpc, String service, String lifeline, String scheme, String web,
        String ns, String nn, FederationNamenodeServiceState state) {
      this.rpcAddress = rpc;
      this.serviceAddress = service;
      this.lifelineAddress = lifeline;
      this.webScheme = scheme;
      this.webAddress = web;
      this.namenodeId = nn;
      this.nameserviceId = ns;
      this.state = state;
      this.dateModified = Time.now();
    }

    public void setState(FederationNamenodeServiceState newState) {
      this.state = newState;
      this.dateModified = Time.now();
    }

    @Override
    public String getRpcAddress() {
      return rpcAddress;
    }

    @Override
    public String getServiceAddress() {
      return serviceAddress;
    }

    @Override
    public String getLifelineAddress() {
      return lifelineAddress;
    }

    @Override
    public String getWebScheme() {
      return webScheme;
    }

    @Override
    public String getWebAddress() {
      return webAddress;
    }

    @Override
    public String getNamenodeKey() {
      return nameserviceId + " " + namenodeId + " " + rpcAddress;
    }

    @Override
    public String getNameserviceId() {
      return nameserviceId;
    }

    @Override
    public String getNamenodeId() {
      return namenodeId;
    }

    @Override
    public FederationNamenodeServiceState getState() {
      return state;
    }

    @Override
    public long getDateModified() {
      return dateModified;
    }
  }

  @Override
  public synchronized boolean registerNamenode(NamenodeStatusReport report)
      throws IOException {
    if (disableRegistration) {
      return false;
    }

    MockNamenodeContext context = new MockNamenodeContext(
        report.getRpcAddress(), report.getServiceAddress(),
        report.getLifelineAddress(), report.getWebScheme(),
        report.getWebAddress(), report.getNameserviceId(),
        report.getNamenodeId(), report.getState());

    String nsId = report.getNameserviceId();
    String bpId = report.getBlockPoolId();
    String cId = report.getClusterId();

    @SuppressWarnings("unchecked")
    List<MockNamenodeContext> existingItems =
        (List<MockNamenodeContext>) this.resolver.get(nsId);
    if (existingItems == null) {
      existingItems = new ArrayList<>();
      this.resolver.put(bpId, existingItems);
      this.resolver.put(nsId, existingItems);
    }
    boolean added = false;
    for (int i=0; i<existingItems.size() && !added; i++) {
      MockNamenodeContext existing = existingItems.get(i);
      if (existing.getNamenodeKey().equals(context.getNamenodeKey())) {
        existingItems.set(i, context);
        added = true;
      }
    }
    if (!added) {
      existingItems.add(context);
    }
    Collections.sort(existingItems, new NamenodePriorityComparator());

    FederationNamespaceInfo info = new FederationNamespaceInfo(bpId, cId, nsId);
    this.namespaces.add(info);
    return true;
  }

  @Override
  public synchronized Set<FederationNamespaceInfo> getNamespaces()
      throws IOException {
    Set<FederationNamespaceInfo> ret = new TreeSet<>();
    Set<String> disabled = getDisabledNamespaces();
    for (FederationNamespaceInfo ns : namespaces) {
      if (!disabled.contains(ns.getNameserviceId())) {
        ret.add(ns);
      }
    }
    return Collections.unmodifiableSet(ret);
  }

  public void clearDisableNamespaces() {
    this.disableNamespaces.clear();
  }

  public void disableNamespace(String nsId) {
    this.disableNamespaces.add(nsId);
  }

  @Override
  public Set<String> getDisabledNamespaces() throws IOException {
    return this.disableNamespaces;
  }

  @Override
  public PathLocation getDestinationForPath(String path) throws IOException {
    List<RemoteLocation> remoteLocations = new LinkedList<>();
    // We go from the leaves to the root
    List<String> keys = new ArrayList<>(this.locations.keySet());
    Collections.sort(keys, Collections.reverseOrder());
    for (String key : keys) {
      if (path.startsWith(key)) {
        for (RemoteLocation location : this.locations.get(key)) {
          String finalPath = location.getDest();
          String extraPath = path.substring(key.length());
          if (finalPath.endsWith("/") && extraPath.startsWith("/")) {
            extraPath = extraPath.substring(1);
          }
          finalPath += extraPath;
          String nameservice = location.getNameserviceId();
          RemoteLocation remoteLocation =
              new RemoteLocation(nameservice, finalPath, path);
          remoteLocations.add(remoteLocation);
        }
        break;
      }
    }
    if (remoteLocations.isEmpty()) {
      // Path isn't supported, mimic resolver behavior.
      return null;
    }
    return new PathLocation(path, remoteLocations);
  }

  @Override
  public List<String> getMountPoints(String path) throws IOException {
    List<String> mountPoints = new ArrayList<>();
    for (String mp : this.locations.keySet()) {
      if (mp.startsWith(path)) {
        mountPoints.add(mp);
      }
    }
    return FileSubclusterResolver.getMountPoints(path, mountPoints);
  }

  @Override
  public void setRouterId(String router) {
  }

  @Override
  public void rotateCache(
      String nsId, FederationNamenodeContext namenode, boolean listObserversFirst) {
  }

  /**
   * Mocks the availability of default namespace.
   * @param b if true default namespace is unset.
   */
  public void setDisableNamespace(boolean b) {
    this.disableDefaultNamespace = b;
  }

  @Override
  public String getDefaultNamespace() {
    if (disableDefaultNamespace) {
      return "";
    }
    return defaultNamespace;
  }

  @Override
  public Set<String> getAllNamespaces() {
    return null;
  }
}
