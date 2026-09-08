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
package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.classification.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Abstract base class for regex-based datanode affinity.
 *
 * <p>Affinity data is stored as groups in a pluggable backing store (for
 * example a JSON file loaded by {@link FileDatanodeAffinityManager}).
 * Each group has:
 * <ul>
 *   <li>{@code regexPattern}  — matched against the HDFS source file path</li>
 *   <li>{@code datanodesRegex} — matched against live cluster datanode hostnames</li>
 * </ul>
 *
 * <h2>Lifecycle</h2>
 * <ol>
 *   <li>{@link DatanodeManager} instantiates the configured implementation,
 *       calls {@link #setDatanodeManager(DatanodeManager)}, then calls
 *       {@link #refresh()}.</li>
 *   <li>{@link #refresh()} delegates raw record loading to the abstract
 *       {@link #loadAffinityRecords()} method, then resolves each record's
 *       {@code datanodesRegex} against all live cluster datanodes and builds
 *       {@link #pathRegexToDataNodeMap} ({@code regexPattern → List<host:port>}).
 *       It then calls {@link DatanodeManager#postAffinityRefresh(Set)} to update
 *       topology membership and rebuild per-group
 *       {@link BlockPlacementPolicies}.</li>
 *   <li>{@link BlockManager} holds one {@link BlockPlacementPolicies} per
 *       affinity group, each backed by a restricted {@link org.apache.hadoop.net.NetworkTopology}
 *       containing only that group's eligible DataNodes.</li>
 * </ol>
 *
 * To enable, set {@code dfs.datanode.affinity.manager.classname} to a concrete
 * implementation class.  If the property is absent the manager is disabled and
 * BlockPlacementPolicy uses its default behaviour.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public abstract class DatanodeAffinityManager implements Configurable {

  private static final Logger LOG =
      LoggerFactory.getLogger(DatanodeAffinityManager.class);

  /**
   * One raw record from the backing store (one DB row or JSON object).
   * The base class uses this to build {@link #pathRegexToDataNodeMap}.
   */
  public static final class AffinityRecord {
    /** Human-readable group name, used for logging. */
    public final String groupName;
    /** Java regex matched against the HDFS source path. */
    public final String regexPattern;
    /** Java regex matched against cluster datanode hostnames. */
    public final String datanodesRegex;

    public AffinityRecord(String groupName, String regexPattern,
        String datanodesRegex) {
      this.groupName     = groupName;
      this.regexPattern  = regexPattern;
      this.datanodesRegex = datanodesRegex;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof AffinityRecord)) {
        return false;
      }
      AffinityRecord that = (AffinityRecord) o;
      return Objects.equals(groupName, that.groupName)
          && Objects.equals(regexPattern, that.regexPattern)
          && Objects.equals(datanodesRegex, that.datanodesRegex);
    }

    @Override
    public int hashCode() {
      return Objects.hash(groupName, regexPattern, datanodesRegex);
    }
  }

  /** Reference to the DatanodeManager for enumerating live cluster nodes. */
  protected DatanodeManager datanodeManager;

  /**
   * Primary map to store file-regex to list of datanodes.
   *
   * <p>Built during {@link #refresh()} by resolving each record's
   * {@code datanodesRegex} against live cluster nodes.  Multiple records whose
   * {@code regexPattern} is identical have their datanode lists merged.
   *
   */
  private volatile Map<String, List<String>> pathRegexToDataNodeMap =
      Collections.emptyMap();

  /**
   * One entry per raw {@link AffinityRecord}: compiled {@code datanodesRegex}
   * paired with the record's {@code regexPattern} (the map key).
   *
   * <p>Used by {@link #onDatanodeRegistered(DatanodeDescriptor)} to decide,
   * for a freshly registered node, which map entries it should be appended to
   * — without reloading anything from the backing store.
   *
   * <p>Rebuilt atomically on every {@link #refresh()}.
   */
  private volatile List<AbstractMap.SimpleEntry<Pattern, String>>
      datanodePatterns = Collections.emptyList();

  /**
   * Union of every {@code "hostname:port"} address that belongs to ANY
   * affinity group.
   *
   * <p>Used by {@link BlockManager} to exclude the entire isolated DataNode
   * pool from block placement when a file path does not match any affinity
   * group's {@code regexPattern}.  This enforces isolation in both directions:
   * affinity paths are pinned to the pool, and non-affinity paths are
   * prevented from using it.
   *
   * <p>Backed by a {@link ConcurrentHashMap} key-set so that incremental
   * appends from {@link #onDatanodeRegistered(DatanodeDescriptor)} are
   * immediately visible without a full rebuild.  The reference itself is
   * atomically replaced on every {@link #refresh()}.
   */
  private volatile Set<String> isolatedDatanodes = ConcurrentHashMap.newKeySet();

  /**
   * Holds a restricted {@link NetworkTopology} for one affinity group.
   *
   * <p>The topology contains only the DataNodes that are eligible for the
   * group identified by {@code pathPattern}.  {@link BlockManager} creates
   * one {@link BlockPlacementPolicies} per group using this topology so that
   * {@link NetworkTopology#chooseRandom} never has to scan a large
   * exclusion list — it only sees the small set of in-pool nodes.
   */
  public static final class AffinityGroupTopology {
    /** File-path regex that identifies this affinity group. */
    public final Pattern pathPattern;
    /** NetworkTopology containing only this group's eligible DataNodes. */
    public final NetworkTopology topology;

    AffinityGroupTopology(Pattern pathPattern, NetworkTopology topology) {
      this.pathPattern = pathPattern;
      this.topology = topology;
    }
  }

  /**
   * One {@link AffinityGroupTopology} per unique {@code regexPattern} key.
   * Replaced atomically on every {@link #refresh()}.
   */
  private volatile List<AffinityGroupTopology> affinityGroupTopologies =
      Collections.emptyList();

  /**
   * Maps {@code regexPattern → NetworkTopology} for the per-group restricted
   * topology.  Kept as a separate field so that {@link #onDatanodeRegistered}
   * can add a new node to the correct topology without scanning the list.
   * Replaced atomically on every {@link #refresh()}.
   */
  private volatile Map<String, NetworkTopology> fileRegexToGroupTopology =
      Collections.emptyMap();

  /**
   * Snapshot of the {@link AffinityRecord} list from the most recent
   * successful {@link #internalRefresh()}.  {@code null} before the first
   * refresh.  Used for idempotency: if the backing store returns an identical
   * set of records, the expensive datanode-resolution and topology-rebuild
   * steps are skipped.
   */
  private volatile List<AffinityRecord> lastLoadedRecords = null;

  /**
   * Inject the {@link DatanodeManager} so the base class can enumerate live
   * cluster nodes during {@link #refresh()}.
   * Called by {@link DatanodeManager} immediately after instantiation.
   */
  public void setDatanodeManager(DatanodeManager dm) {
    this.datanodeManager = dm;
  }

  /**
   * Load raw affinity records from the backing store (DB rows or JSON objects).
   *
   * <p>The base class calls this from {@link #refresh()} and uses the returned
   * records to build {@link #pathRegexToDataNodeMap}.  Implementations do NOT
   * need to resolve datanodes — that is done here.
   *
   * @return non-null list of raw affinity records (may be empty)
   * @throws IOException if the backing store cannot be accessed
   */
  protected abstract List<AffinityRecord> loadAffinityRecords() throws IOException;

  /**
   * Reload affinity data and rebuild {@link #pathRegexToDataNodeMap}.
   *
   * <p>Steps:
   * <ol>
   *   <li>Call {@link #loadAffinityRecords()} to fetch raw records.</li>
   *   <li>For each record compile {@code datanodesRegex} and match it against
   *       every hostname returned by {@link DatanodeManager#getAllDatanodes()};
   *       collect matching hostnames as {@code "host:port"} strings.</li>
   *   <li>Build {@code regexPattern → List<host:port>}, merging lists when
   *       multiple records share the same {@code regexPattern}.</li>
   *   <li>Pre-compile the {@code regexPattern} keys for fast srcPath lookup.</li>
   * </ol>
   *
   * <p>Subclasses that need pre/post processing (e.g. table creation on first
   * call) should override this method and call {@code super.refresh()}.
   *
   */
  public void refresh() {
    try {
      internalRefresh();
    } catch (Exception e) {
      LOG.warn("Failed to refresh affinity datanode manager ", e);
    }
  }

  private void internalRefresh() throws IOException {
    List<AffinityRecord> records = loadAffinityRecords();

    // Idempotency: skip the expensive datanode-resolution and topology-rebuild
    // when the backing store returns exactly the same records as last time.
    // The comparison is ORDER-SENSITIVE (List.equals): affinity group
    // precedence is "first declared wins", so a pure reorder of overlapping
    // rules is a semantic config change that must trigger a rebuild.
    List<AffinityRecord> previous = this.lastLoadedRecords;
    if (previous != null && records.equals(previous)) {
      LOG.debug("DatanodeAffinityManager: records unchanged ({} record(s)),"
          + " skipping rebuild", records.size());
      // Even when the affinity records are identical, the set of live
      // DataNodes may have changed since the last full rebuild (e.g. a node
      // restarted, decommissioned, or was added between refreshes).  Re-run
      // the default-topology reconciliation against the current isolated set
      // so an explicit -refreshNodes always leaves the default topology
      // consistent instead of returning early with stale membership.
      if (datanodeManager != null && this.isolatedDatanodes != null) {
        datanodeManager.postAffinityRefresh(this.isolatedDatanodes);
      }
      return;
    }

    Collection<DatanodeDescriptor> allDatanodes = datanodeManager != null
        ? datanodeManager.getAllDatanodes()
        : Collections.emptyList();

    // Use ConcurrentHashMap so that onDatanodeRegistered() can safely read
    // entries concurrently.  Values are CopyOnWriteArrayList so individual
    // appends by onDatanodeRegistered() are thread-safe without locking the
    // whole map.
    Map<String, List<String>> newMap =
        new ConcurrentHashMap<>();

    // Per-group restricted NetworkTopology: contains only the eligible nodes
    // for each affinity group.  ConcurrentHashMap for safe concurrent reads.
    Map<String, NetworkTopology> newRegexToTopology = new ConcurrentHashMap<>();

    // Compiled (datanodePattern → fileRegex) pairs for incremental updates.
    List<AbstractMap.SimpleEntry<Pattern, String>> newDnPatterns =
        new ArrayList<>();

    // Distinct path regexes in declaration order. Group precedence for an
    // overlapping path is "first declared wins" (findAffinityGroup returns the
    // first match), so this list -- not the unordered ConcurrentHashMap -- must
    // drive the order of affinityGroupTopologies to keep placement
    // deterministic across refreshes.
    LinkedHashSet<String> orderedRegexes = new LinkedHashSet<>();

    for (AffinityRecord record : records) {
      try {
        // Skip records missing required fields BEFORE compiling: a null regex
        // would throw NullPointerException (not PatternSyntaxException) and
        // abort the whole refresh, leaving stale affinity state in place.
        if (record.regexPattern == null || record.regexPattern.isEmpty()
            || record.datanodesRegex == null
            || record.datanodesRegex.isEmpty()) {
          LOG.warn("DatanodeAffinityManager: skipping record in group '{}' with "
              + "null/empty regexPattern or datanodesRegex", record.groupName);
          continue;
        }
        // Validate the file-path regex (the map KEY) up front so that a record
        // with an invalid regexPattern is dropped ENTIRELY -- otherwise its
        // nodes would be accumulated into newMap/newIsolated (isolated from the
        // default topology) while the group is later skipped for having no
        // compilable path pattern, leaving those nodes isolated with no usable
        // placement policy.
        Pattern.compile(record.regexPattern);
        Pattern datanodePattern = Pattern.compile(record.datanodesRegex);
        // Track pattern for onDatanodeRegistered().
        newDnPatterns.add(new AbstractMap.SimpleEntry<>(
            datanodePattern, record.regexPattern));
        orderedRegexes.add(record.regexPattern);

        // computeIfAbsent: multiple records with the same regexPattern share
        // one CopyOnWriteArrayList (their node sets are merged).
        List<String> nodeList = newMap.computeIfAbsent(
            record.regexPattern, k -> new CopyOnWriteArrayList<>());

        // Per-group topology: also share one NetworkTopology per regexPattern.
        final NetworkTopology groupTopo = newRegexToTopology.computeIfAbsent(
            record.regexPattern, k -> {
              NetworkTopology t;
              try {
                t = datanodeManager != null
                    ? datanodeManager.createEmptyTopology()
                    : null;
              } catch (IOException e) {
                LOG.warn(
                    "Failed to create network-topology instance..not using datanode affinity");
                throw new RuntimeException(e);
              }
              return (t != null) ? t : new NetworkTopology();
            });

        for (DatanodeDescriptor dn : allDatanodes) {
          // getXferAddrWithHostname() returns "hostname:port".
          String xferAddr = dn.getXferAddrWithHostname();
          if (xferAddr != null && datanodePattern.matcher(xferAddr).find()) {
            if (!nodeList.contains(xferAddr)) {
              nodeList.add(xferAddr);
            }
            // Add to the restricted topology for this group.
            // Wrap in try/catch: real DatanodeDescriptors always succeed;
            // mock objects in unit tests may lack network-location info.
            try {
              groupTopo.add(dn);
            } catch (Exception e) {
              LOG.debug("DatanodeAffinityManager: could not add {} to affinity"
                  + " group topology (will fall back to default placement): {}",
                  xferAddr, e.getMessage());
            }
          }
        }
      } catch (PatternSyntaxException e) {
        LOG.warn("DatanodeAffinityManager: skipping record in group '{}' with "
            + "invalid regexPattern/datanodesRegex: {}",
            record.groupName, e.getMessage());
      }
    }

    // Build affinityGroupTopologies in declaration order (first-declared wins
    // for overlapping path patterns) so group precedence is deterministic
    // across refreshes.
    List<AffinityGroupTopology> newGroupTopologyList = new ArrayList<>();
    for (String regex : orderedRegexes) {
      try {
        Pattern pathPattern = Pattern.compile(regex);
        NetworkTopology topo = newRegexToTopology.get(regex);
        if (topo != null) {
          newGroupTopologyList.add(new AffinityGroupTopology(pathPattern, topo));
        }
      } catch (PatternSyntaxException e) {
        LOG.warn("DatanodeAffinityManager: skipping invalid regexPattern '{}': {}",
            regex, e.getMessage());
      }
    }

    // Build the union of all affinity pool addresses for reverse exclusion:
    // when a path does not match any group, these nodes are excluded from
    // block placement so non-affinity writes cannot land on the isolated pool.
    Set<String> newIsolated = ConcurrentHashMap.newKeySet();
    for (List<String> nodes : newMap.values()) {
      newIsolated.addAll(nodes);
    }

    // Publish order matters for the lock-free onDatanodeRegistered() reader.
    // That reader gates on datanodePatterns FIRST and, only for a matched
    // pattern, reads pathRegexToDataNodeMap and fileRegexToGroupTopology. By
    // publishing datanodePatterns LAST, the Java Memory Model guarantees that
    // any thread observing the new datanodePatterns (a volatile read) also
    // observes the new maps/topologies written before it (happens-before via
    // the ordered volatile writes). This closes the torn-read window in which
    // a concurrently-registering DataNode would otherwise be deferred and could
    // linger in the default topology (an isolation leak).
    // The unchecked cast is safe: ConcurrentHashMap<String,
    // CopyOnWriteArrayList<String>> satisfies Map<String, List<String>> at
    // runtime.
    this.pathRegexToDataNodeMap =
        (Map<String, List<String>>) (Map<?, ?>) newMap;
    this.isolatedDatanodes = newIsolated;
    this.fileRegexToGroupTopology = newRegexToTopology;
    this.affinityGroupTopologies =
        Collections.unmodifiableList(newGroupTopologyList);
    // Published LAST: gates onDatanodeRegistered() and thus establishes
    // happens-before for all the fields written above.
    this.datanodePatterns = Collections.unmodifiableList(newDnPatterns);
    // Record the snapshot used for this build so the next refresh() can
    // skip the rebuild if the backing store has not changed.
    this.lastLoadedRecords = Collections.unmodifiableList(new ArrayList<>(records));

    // Close the symmetric "removal race": internalRefresh() runs lock-free, so
    // a DataNode can be removed (removeDatanode -> onDatanodeRemoved) AFTER the
    // getAllDatanodes() snapshot above but BEFORE this publication. Such a node
    // was built into the freshly published structures from the stale snapshot,
    // while onDatanodeRemoved() pruned only the PREVIOUSLY published structures
    // -- so without this it would linger as a dead, unreachable node in the new
    // group topology / isolated set (and postAffinityRefresh() iterates only
    // live nodes, so it never purges it). We still hold the snapshot
    // descriptors, so re-running onDatanodeRemoved() against the now-published
    // structures cleanly removes any snapshot node that is no longer live.
    // Combined with removeDatanode's own onDatanodeRemoved() on the published
    // structures, this closes the window for all practical interleavings; any
    // node removed after the fresh check below is handled by that hook instead.
    if (datanodeManager != null && !allDatanodes.isEmpty()) {
      Set<String> stillLive = ConcurrentHashMap.newKeySet();
      for (DatanodeDescriptor dn : datanodeManager.getAllDatanodes()) {
        if (dn != null) {
          String liveAddr = dn.getXferAddrWithHostname();
          if (liveAddr != null) {
            stillLive.add(liveAddr);
          }
        }
      }
      for (DatanodeDescriptor dn : allDatanodes) {
        if (dn == null) {
          continue;
        }
        String addr = dn.getXferAddrWithHostname();
        if (addr != null && !stillLive.contains(addr)) {
          onDatanodeRemoved(dn);
        }
      }
    }

    // Notify DatanodeManager so it can update topology membership:
    // remove newly isolated nodes from the default NetworkTopology (so the
    // default BlockPlacementPolicy can never select them) and re-add nodes
    // that are no longer in any affinity group.  Also triggers
    // BlockManager.rebuildAffinityPolicies().
    if (datanodeManager != null) {
      datanodeManager.postAffinityRefresh(newIsolated);
    }

    LOG.info("DatanodeAffinityManager: built {} path-regex → datanode mapping(s)",
        newMap.size());
    if (LOG.isDebugEnabled()) {
      newMap.forEach((r, datanodes) ->
          LOG.debug("DatanodeAffinityManager: '{}' → {} node(s): {}",
              r, datanodes.size(), datanodes));
    }
  }

  /**
   * Incrementally add a newly registered DataNode to the affinity map.
   *
   * <p>Called by {@link DatanodeManager#registerDatanode} on every successful
   * DataNode registration (both brand-new nodes and re-registrations) so that
   * block-placement affinity is effective immediately, without waiting for the
   * next periodic {@link #refresh()}.
   *
   * <p>For every affinity group whose compiled {@code datanodesRegex} matches
   * the node's {@link DatanodeDescriptor#getXferAddrWithHostname()}, the address
   * is appended to that group's {@link List} in
   * {@link #pathRegexToDataNodeMap} and the node is added to the per-group
   * restricted {@link NetworkTopology}.
   *
   * <p>This method is a no-op when {@link #refresh()} has not yet been called
   * (empty {@link #datanodePatterns}).
   *
   * <p><b>Idempotent and concurrency-safe.</b> Besides the registration path it
   * is also invoked from {@link DatanodeManager#postAffinityRefresh} as an
   * authoritative reconciliation step for every live DataNode, so it may run
   * concurrently with a real registration for the same node. Membership is
   * therefore updated with an atomic add-if-absent; a node already tracked is
   * not inserted twice.
   *
   * @param dn the DataNode descriptor that just completed registration
   * @return {@code true} if the DataNode matched at least one affinity group
   *         and should therefore be removed from the default
   *         {@link NetworkTopology} by {@link DatanodeManager}
   */
  public boolean onDatanodeRegistered(DatanodeDescriptor dn) {
    try {
      boolean isAffinityNode = false;
      List<AbstractMap.SimpleEntry<Pattern, String>> patterns =
          this.datanodePatterns;
      if (patterns.isEmpty()) {
        return false;
      }
      String xferAddr = dn.getXferAddrWithHostname();
      if (xferAddr == null) {
        return false;
      }
      boolean matched = false;
      for (AbstractMap.SimpleEntry<Pattern, String> entry : patterns) {
        if (entry.getKey().matcher(xferAddr).find()) {
          String fileRegex = entry.getValue();
          List<String> list = pathRegexToDataNodeMap.get(fileRegex);
          NetworkTopology topo = fileRegexToGroupTopology.get(fileRegex);
          // Defensive coherence guard. refresh() publishes datanodePatterns
          // LAST, so observing the new patterns here (the loop we are in)
          // establishes happens-before for pathRegexToDataNodeMap and
          // fileRegexToGroupTopology -- both are guaranteed non-null for a
          // matched pattern. This branch should therefore not trigger in
          // practice; it remains only to guarantee we never report a match we
          // cannot back with a group topology (which would make DatanodeManager
          // remove the node from the default topology while leaving it out of
          // the group topology, orphaning it in neither). If it ever fires,
          // leave the node in the default topology until the next refresh.
          if (list == null || topo == null) {
            LOG.debug("DatanodeAffinityManager: affinity structures for '{}'"
                + " not yet published while registering {}; deferring"
                + " isolation to the next refresh", fileRegex, xferAddr);
            continue;
          }
          // The node belongs to an affinity group and MUST be isolated from
          // the default topology.  Report the match regardless of whether the
          // node is inserted below: on a DataNode re-registration the address
          // is already tracked, so the insert is skipped, but the caller must
          // still remove it from the default topology (otherwise a restarted
          // isolated DataNode leaks back into default placement).
          matched = true;
          // Atomically add-if-absent so this method is safe to invoke
          // concurrently from a DataNode registration AND from the refresh
          // reconciliation pass (DatanodeManager.postAffinityRefresh) without
          // producing a duplicate entry. The map values are always
          // CopyOnWriteArrayList (published by internalRefresh), whose
          // addIfAbsent is atomic; fall back to a monitor-guarded contains/add
          // for any other List type.
          final boolean newlyAdded;
          if (list instanceof CopyOnWriteArrayList) {
            newlyAdded =
                ((CopyOnWriteArrayList<String>) list).addIfAbsent(xferAddr);
          } else {
            synchronized (list) {
              newlyAdded = !list.contains(xferAddr);
              if (newlyAdded) {
                list.add(xferAddr);
              }
            }
          }
          if (newlyAdded) {
            // Keep the global isolated-pool set in sync.
            isolatedDatanodes.add(xferAddr);
            // Also add to the per-group restricted topology so the group's
            // BlockPlacementPolicies sees the new node immediately.
            try {
              topo.add(dn);
              LOG.info("DatanodeAffinityManager: registered DataNode {} added to "
                  + "affinity group for '{}'", xferAddr, fileRegex);
            } catch (Exception e) {
              LOG.warn(
                  "DatanodeAffinityManager: could not add {} to affinity"
                      + " group topology on registration: {}",
                  xferAddr, e.getMessage());
            }
          } else {
            // The address is already tracked, but the per-group topology stores
            // descriptor OBJECTS, not addresses. On a DataNode *replacement*
            // (same host:port re-registering with a NEW descriptor/UUID after a
            // disk wipe or host reimage) -- especially when the currently
            // published structures were built from an earlier refresh snapshot
            // that still referenced the previous descriptor -- the topology can
            // hold a STALE descriptor for this address, which placement would
            // then select as a dead target. Reconcile the topology to the live
            // descriptor. Object-identity guarded, so it is a no-op for the
            // common re-registration case where the same descriptor object is
            // reused (updateRegInfo mutates it in place).
            ensureLiveGroupDescriptor(topo, dn, xferAddr);
          }
        }
      }
      return matched;
    } catch (Exception e) {
      LOG.warn("Failed to register datanode to affinity {}, {}", dn,
          e.getMessage());
    }
    return false;
  }

  /**
   * Reconcile a per-group restricted {@link NetworkTopology} so it holds the
   * LIVE {@link DatanodeDescriptor} for a transfer address that is already
   * tracked in the group. This closes the DataNode-replacement window: a new
   * descriptor (new storage UUID) reusing an address that a stale descriptor
   * still occupies in the topology could otherwise be selected as a dead
   * placement target.
   *
   * <p>Object-identity guarded: a no-op when the topology already holds this
   * exact descriptor (the common re-registration path reuses the same object,
   * mutated in place by {@code updateRegInfo}). Otherwise it removes EVERY stale
   * descriptor for the same transfer endpoint -- matched by
   * {@link DatanodeDescriptor#getXferAddrWithHostname()}, the affinity identity,
   * regardless of the network path it sits under -- and then inserts the live
   * one. Matching by endpoint rather than by exact path is required because a
   * replacement may resolve to a different rack/IP, so the stale leaf can live
   * at a different path than the live descriptor and a path-only lookup would
   * miss it. The per-group topology holds only this group's nodes, so the scan
   * is cheap. Best-effort and exception-safe; any transient inconsistency
   * self-heals on the next {@link #refresh()}.
   *
   * @param topo     the per-group restricted topology to reconcile
   * @param dn       the live DataNode descriptor that must be present
   * @param xferAddr the node's transfer address (affinity identity, for match
   *                 and logging)
   */
  private void ensureLiveGroupDescriptor(NetworkTopology topo,
      DatanodeDescriptor dn, String xferAddr) {
    if (topo == null || dn == null || xferAddr == null) {
      return;
    }
    try {
      if (topo.getNode(NodeBase.getPath(dn)) == dn) {
        // Topology already holds the live descriptor at its current path: this
        // is the common steady-state (no replacement) case, so avoid the scan.
        return;
      }
      // A different descriptor (or none) occupies this node's path. Remove every
      // stale leaf for the same transfer endpoint -- including one that a
      // rack/IP change on replacement moved to a different path -- then insert
      // the live descriptor.
      boolean removedStale = false;
      for (Node leaf : topo.getLeaves(NodeBase.ROOT)) {
        if (leaf != dn && leaf instanceof DatanodeDescriptor
            && xferAddr.equals(
                ((DatanodeDescriptor) leaf).getXferAddrWithHostname())) {
          topo.remove(leaf);
          removedStale = true;
        }
      }
      topo.add(dn);
      if (removedStale) {
        LOG.info("DatanodeAffinityManager: reconciled affinity group topology to "
            + "the live descriptor for replaced DataNode {}", xferAddr);
      }
    } catch (Exception e) {
      LOG.warn("DatanodeAffinityManager: could not reconcile affinity group "
          + "topology to the live descriptor for {}: {}", xferAddr,
          e.getMessage());
    }
  }

  /**
   * Hook invoked by {@link DatanodeManager} when a DataNode is removed
   * (decommissioned, dead, or otherwise deregistered). Prunes the node from
   * every affinity structure so a stale, unreachable node is no longer offered
   * as an affinity placement target and does not linger in a group's
   * restricted {@link NetworkTopology}.
   *
   * <p>Idempotent and safe to call for non-affinity nodes (a node that never
   * matched a group is simply absent from every structure).
   *
   * @param dn the DataNode being removed
   */
  public void onDatanodeRemoved(DatanodeDescriptor dn) {
    try {
      if (dn == null) {
        return;
      }
      String xferAddr = dn.getXferAddrWithHostname();
      if (xferAddr == null) {
        return;
      }
      // Remove from the global isolated-pool set.
      isolatedDatanodes.remove(xferAddr);
      // Remove from every per-group address list.
      for (List<String> list : pathRegexToDataNodeMap.values()) {
        if (list != null) {
          list.remove(xferAddr);
        }
      }
      // Remove from every per-group restricted topology. NetworkTopology.remove
      // is a no-op if the node is absent, so this is safe for non-affinity
      // nodes and for groups the node never belonged to.
      for (NetworkTopology topo : fileRegexToGroupTopology.values()) {
        if (topo != null) {
          try {
            topo.remove(dn);
          } catch (Exception e) {
            LOG.warn("DatanodeAffinityManager: could not remove {} from an"
                + " affinity group topology: {}", xferAddr, e.getMessage());
          }
        }
      }
    } catch (Exception e) {
      LOG.warn("Failed to remove datanode from affinity {}, {}", dn,
          e.getMessage());
    }
  }

  /**
   * Return the current {@code regexPattern → List<host:port>} map.
   * Intended for monitoring and unit tests.
   */
  public Map<String, List<String>> getFileRegexToDataNodeMap() {
    return pathRegexToDataNodeMap;
  }

  /**
   * Return the union of all {@code "hostname:port"} addresses that belong to
   * any affinity group.
   *
   * <p>Used by {@link BlockManager} to build the excluded-nodes set for file
   * paths that do not match any affinity group, ensuring that isolated
   * DataNodes are never used for non-affinity writes.
   *
   * @return unmodifiable view of the isolated DataNode address set; empty when
   *         no affinity groups have been loaded
   */
  public Set<String> getIsolatedDatanodes() {
    return Collections.unmodifiableSet(isolatedDatanodes);
  }

  /**
   * Directly overwrite {@link #pathRegexToDataNodeMap} and rebuild the
   * compiled lookup.  Intended for unit tests that want to inject a pre-built
   * map without going through a real backing store or DatanodeManager.
   *
   * @param map {@code regexPattern → List<"host:port">}
   */
  @SuppressWarnings("unchecked")
  @VisibleForTesting
  public void setFileRegexToDataNodeMap(Map<String, List<String>> map) {
    Map<String, List<String>> newMap = new ConcurrentHashMap<>();
    for (Map.Entry<String, List<String>> entry : map.entrySet()) {
      newMap.put(entry.getKey(), new CopyOnWriteArrayList<>(entry.getValue()));
    }
    Set<String> newIsolated = ConcurrentHashMap.newKeySet();
    for (List<String> nodes : newMap.values()) {
      newIsolated.addAll(nodes);
    }
    this.pathRegexToDataNodeMap =
        (Map<String, List<String>>) (Map<?, ?>) newMap;
    this.isolatedDatanodes = newIsolated;
    // datanodePatterns intentionally not set: this hook bypasses refresh().
    // Clear per-group topologies: tests use the map-lookup path.
    this.fileRegexToGroupTopology = Collections.emptyMap();
    this.affinityGroupTopologies = Collections.emptyList();
  }

  /**
   * Return the per-group restricted topologies for use by {@link BlockManager}.
   *
   * <p>Each entry's {@link AffinityGroupTopology#topology} contains only the
   * DataNodes eligible for that group's path pattern.
   *
   * @return immutable snapshot; empty when no affinity groups are loaded or
   *         when the test injection path ({@link #setFileRegexToDataNodeMap})
   *         was used
   */
  public List<AffinityGroupTopology> getAffinityGroupTopologies() {
    return affinityGroupTopologies;
  }
}
