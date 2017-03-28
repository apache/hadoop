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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import org.apache.hadoop.hdfs.server.federation.store.StateStoreService;
import org.apache.hadoop.util.Time;

/**
 * In-memory cache/mock of a namenode and file resolver. Stores the most
 * recently updated NN information for each nameservice and block pool. Also
 * stores a virtual mount table for resolving global namespace paths to local NN
 * paths.
 */
public class MockResolver
    implements ActiveNamenodeResolver, FileSubclusterResolver {

  private Map<String, List<? extends FederationNamenodeContext>> resolver =
      new HashMap<String, List<? extends FederationNamenodeContext>>();
  private Map<String, List<RemoteLocation>> locations =
      new HashMap<String, List<RemoteLocation>>();
  private Set<FederationNamespaceInfo> namespaces =
      new HashSet<FederationNamespaceInfo>();
  private String defaultNamespace = null;

  public MockResolver(Configuration conf, StateStoreService store) {
    this.cleanRegistrations();
  }

  public void addLocation(String mount, String nameservice, String location) {
    RemoteLocation remoteLocation = new RemoteLocation(nameservice, location);
    List<RemoteLocation> locationsList = locations.get(mount);
    if (locationsList == null) {
      locationsList = new LinkedList<RemoteLocation>();
      locations.put(mount, locationsList);
    }
    if (!locationsList.contains(remoteLocation)) {
      locationsList.add(remoteLocation);
    }

    if (this.defaultNamespace == null) {
      this.defaultNamespace = nameservice;
    }
  }

  public synchronized void cleanRegistrations() {
    this.resolver =
        new HashMap<String, List<? extends FederationNamenodeContext>>();
    this.namespaces = new HashSet<FederationNamespaceInfo>();
  }

  @Override
  public void updateActiveNamenode(
      String ns, InetSocketAddress successfulAddress) {

    String address = successfulAddress.getHostName() + ":" +
        successfulAddress.getPort();
    String key = ns;
    if (key != null) {
      // Update the active entry
      @SuppressWarnings("unchecked")
      List<FederationNamenodeContext> iterator =
          (List<FederationNamenodeContext>) resolver.get(key);
      for (FederationNamenodeContext namenode : iterator) {
        if (namenode.getRpcAddress().equals(address)) {
          MockNamenodeContext nn = (MockNamenodeContext) namenode;
          nn.setState(FederationNamenodeServiceState.ACTIVE);
          break;
        }
      }
      Collections.sort(iterator, new NamenodePriorityComparator());
    }
  }

  @Override
  public List<? extends FederationNamenodeContext>
      getNamenodesForNameserviceId(String nameserviceId) {
    return resolver.get(nameserviceId);
  }

  @Override
  public List<? extends FederationNamenodeContext> getNamenodesForBlockPoolId(
      String blockPoolId) {
    return resolver.get(blockPoolId);
  }

  private static class MockNamenodeContext
      implements FederationNamenodeContext {
    private String webAddress;
    private String rpcAddress;
    private String serviceAddress;
    private String lifelineAddress;
    private String namenodeId;
    private String nameserviceId;
    private FederationNamenodeServiceState state;
    private long dateModified;

    MockNamenodeContext(
        String rpc, String service, String lifeline, String web,
        String ns, String nn, FederationNamenodeServiceState state) {
      this.rpcAddress = rpc;
      this.serviceAddress = service;
      this.lifelineAddress = lifeline;
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
    MockNamenodeContext context = new MockNamenodeContext(
        report.getRpcAddress(), report.getServiceAddress(),
        report.getLifelineAddress(), report.getWebAddress(),
        report.getNameserviceId(), report.getNamenodeId(), report.getState());

    String nsId = report.getNameserviceId();
    String bpId = report.getBlockPoolId();
    String cId = report.getClusterId();
    @SuppressWarnings("unchecked")
    List<MockNamenodeContext> existingItems =
        (List<MockNamenodeContext>) resolver.get(nsId);
    if (existingItems == null) {
      existingItems = new ArrayList<MockNamenodeContext>();
      resolver.put(bpId, existingItems);
      resolver.put(nsId, existingItems);
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
    namespaces.add(info);
    return true;
  }

  @Override
  public Set<FederationNamespaceInfo> getNamespaces() throws IOException {
    return this.namespaces;
  }

  @Override
  public PathLocation getDestinationForPath(String path) throws IOException {
    String finalPath = null;
    String nameservice = null;
    Set<String> namespaceSet = new HashSet<String>();
    LinkedList<RemoteLocation> remoteLocations =
        new LinkedList<RemoteLocation>();
    for(String key : this.locations.keySet()) {
      if(path.startsWith(key)) {
        for (RemoteLocation location : this.locations.get(key)) {
          finalPath = location.getDest() + path.substring(key.length());
          nameservice = location.getNameserviceId();
          RemoteLocation remoteLocation =
              new RemoteLocation(nameservice, finalPath);
          remoteLocations.add(remoteLocation);
          namespaceSet.add(nameservice);
        }
        break;
      }
    }
    if (remoteLocations.isEmpty()) {
      // Path isn't supported, mimic resolver behavior.
      return null;
    }
    return new PathLocation(path, remoteLocations, namespaceSet);
  }

  @Override
  public List<String> getMountPoints(String path) throws IOException {
    List<String> mounts = new ArrayList<String>();
    if (path.equals("/")) {
      // Mounts only supported under root level
      for (String mount : this.locations.keySet()) {
        if (mount.length() > 1) {
          // Remove leading slash, this is the behavior of the mount tree,
          // return only names.
          mounts.add(mount.replace("/", ""));
        }
      }
    }
    return mounts;
  }

  @Override
  public void setRouterId(String router) {
  }

  @Override
  public String getDefaultNamespace() {
    return defaultNamespace;
  }
}
