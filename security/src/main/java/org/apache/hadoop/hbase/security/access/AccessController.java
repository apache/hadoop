/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.security.access;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;
import org.apache.hadoop.hbase.ipc.HBaseRPC;
import org.apache.hadoop.hbase.ipc.ProtocolSignature;
import org.apache.hadoop.hbase.ipc.RequestContext;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;

/**
 * Provides basic authorization checks for data access and administrative
 * operations.
 *
 * <p>
 * {@code AccessController} performs authorization checks for HBase operations
 * based on:
 * <ul>
 *   <li>the identity of the user performing the operation</li>
 *   <li>the scope over which the operation is performed, in increasing
 *   specificity: global, table, column family, or qualifier</li>
 *   <li>the type of action being performed (as mapped to
 *   {@link Permission.Action} values)</li>
 * </ul>
 * If the authorization check fails, an {@link AccessDeniedException}
 * will be thrown for the operation.
 * </p>
 *
 * <p>
 * To perform authorization checks, {@code AccessController} relies on the
 * {@link org.apache.hadoop.hbase.ipc.SecureRpcEngine} being loaded to provide
 * the user identities for remote requests.
 * </p>
 *
 * <p>
 * The access control lists used for authorization can be manipulated via the
 * exposed {@link AccessControllerProtocol} implementation, and the associated
 * {@code grant}, {@code revoke}, and {@code user_permission} HBase shell
 * commands.
 * </p>
 */
public class AccessController extends BaseRegionObserver
    implements MasterObserver, AccessControllerProtocol {
  /**
   * Represents the result of an authorization check for logging and error
   * reporting.
   */
  private static class AuthResult {
    private final boolean allowed;
    private final byte[] table;
    private final byte[] family;
    private final byte[] qualifier;
    private final Permission.Action action;
    private final String reason;
    private final User user;

    public AuthResult(boolean allowed, String reason,  User user,
        Permission.Action action, byte[] table, byte[] family, byte[] qualifier) {
      this.allowed = allowed;
      this.reason = reason;
      this.user = user;
      this.table = table;
      this.family = family;
      this.qualifier = qualifier;
      this.action = action;
    }

    public boolean isAllowed() { return allowed; }

    public User getUser() { return user; }

    public String getReason() { return reason; }

    public String toContextString() {
      return "(user=" + (user != null ? user.getName() : "UNKNOWN") + ", " +
          "scope=" + (table == null ? "GLOBAL" : Bytes.toString(table)) + ", " +
          "family=" + (family != null ? Bytes.toString(family) : "") + ", " +
          "qualifer=" + (qualifier != null ? Bytes.toString(qualifier) : "") + ", " +
          "action=" + (action != null ? action.toString() : "") + ")";
    }

    public String toString() {
      return new StringBuilder("AuthResult")
          .append(toContextString()).toString();
    }

    public static AuthResult allow(String reason, User user,
        Permission.Action action, byte[] table) {
      return new AuthResult(true, reason, user, action, table, null, null);
    }

    public static AuthResult deny(String reason, User user,
        Permission.Action action, byte[] table) {
      return new AuthResult(false, reason, user, action, table, null, null);
    }

    public static AuthResult deny(String reason, User user,
        Permission.Action action, byte[] table, byte[] family, byte[] qualifier) {
      return new AuthResult(false, reason, user, action, table, family, qualifier);
    }
  }

  public static final Log LOG = LogFactory.getLog(AccessController.class);

  private static final Log AUDITLOG =
    LogFactory.getLog("SecurityLogger."+AccessController.class.getName());

  /**
   * Version number for AccessControllerProtocol
   */
  private static final long PROTOCOL_VERSION = 1L;

  TableAuthManager authManager = null;

  // flags if we are running on a region of the _acl_ table
  boolean aclRegion = false;

  // defined only for Endpoint implementation, so it can have way to
  // access region services.
  private RegionCoprocessorEnvironment regionEnv;

  /** Mapping of scanner instances to the user who created them */
  private Map<InternalScanner,String> scannerOwners =
      new MapMaker().weakKeys().makeMap();

  void initialize(RegionCoprocessorEnvironment e) throws IOException {
    final HRegion region = e.getRegion();

    Map<byte[],ListMultimap<String,TablePermission>> tables =
        AccessControlLists.loadAll(region);
    // For each table, write out the table's permissions to the respective
    // znode for that table.
    for (Map.Entry<byte[],ListMultimap<String,TablePermission>> t:
      tables.entrySet()) {
      byte[] table = t.getKey();
      String tableName = Bytes.toString(table);
      ListMultimap<String,TablePermission> perms = t.getValue();
      byte[] serialized = AccessControlLists.writePermissionsAsBytes(perms,
          e.getRegion().getConf());
      this.authManager.getZKPermissionWatcher().writeToZookeeper(tableName,
        serialized);
    }
  }

  /**
   * Writes all table ACLs for the tables in the given Map up into ZooKeeper
   * znodes.  This is called to synchronize ACL changes following {@code _acl_}
   * table updates.
   */
  void updateACL(RegionCoprocessorEnvironment e,
      final Map<byte[], List<KeyValue>> familyMap) {
    Set<String> tableSet = new HashSet<String>();
    for (Map.Entry<byte[], List<KeyValue>> f : familyMap.entrySet()) {
      List<KeyValue> kvs = f.getValue();
      for (KeyValue kv: kvs) {
        if (Bytes.compareTo(kv.getBuffer(), kv.getFamilyOffset(),
            kv.getFamilyLength(), AccessControlLists.ACL_LIST_FAMILY, 0,
            AccessControlLists.ACL_LIST_FAMILY.length) == 0) {
          String tableName = Bytes.toString(kv.getRow());
          tableSet.add(tableName);
        }
      }
    }

    for (String tableName: tableSet) {
      try {
        ListMultimap<String,TablePermission> perms =
          AccessControlLists.getTablePermissions(regionEnv.getConfiguration(),
              Bytes.toBytes(tableName));
        byte[] serialized = AccessControlLists.writePermissionsAsBytes(
            perms, e.getRegion().getConf());
        this.authManager.getZKPermissionWatcher().writeToZookeeper(tableName,
          serialized);
      } catch (IOException ex) {
        LOG.error("Failed updating permissions mirror for '" + tableName +
          "'", ex);
      }
    }
  }

  /**
   * Check the current user for authorization to perform a specific action
   * against the given set of row data.
   *
   * <p>Note: Ordering of the authorization checks
   * has been carefully optimized to short-circuit the most common requests
   * and minimize the amount of processing required.</p>
   *
   * @param permRequest the action being requested
   * @param e the coprocessor environment
   * @param families the map of column families to qualifiers present in
   * the request
   * @return
   */
  AuthResult permissionGranted(User user, TablePermission.Action permRequest,
      RegionCoprocessorEnvironment e,
      Map<byte [], ? extends Collection<?>> families) {
    HRegionInfo hri = e.getRegion().getRegionInfo();
    HTableDescriptor htd = e.getRegion().getTableDesc();
    byte[] tableName = hri.getTableName();

    // 1. All users need read access to .META. and -ROOT- tables.
    // this is a very common operation, so deal with it quickly.
    if ((hri.isRootRegion() || hri.isMetaRegion()) &&
        (permRequest == TablePermission.Action.READ)) {
      return AuthResult.allow("All users allowed", user, permRequest,
          hri.getTableName());
    }

    if (user == null) {
      return AuthResult.deny("No user associated with request!", null,
          permRequest, hri.getTableName());
    }

    // 2. The table owner has full privileges
    String owner = htd.getOwnerString();
    if (user.getShortName().equals(owner)) {
      // owner of the table has full access
      return AuthResult.allow("User is table owner", user, permRequest,
          hri.getTableName());
    }

    // 3. check for the table-level, if successful we can short-circuit
    if (authManager.authorize(user, tableName, (byte[])null, permRequest)) {
      return AuthResult.allow("Table permission granted", user,
          permRequest, tableName);
    }

    // 4. check permissions against the requested families
    if (families != null && families.size() > 0) {
      // all families must pass
      for (Map.Entry<byte [], ? extends Collection<?>> family : families.entrySet()) {
        // a) check for family level access
        if (authManager.authorize(user, tableName, family.getKey(),
            permRequest)) {
          continue;  // family-level permission overrides per-qualifier
        }

        // b) qualifier level access can still succeed
        if ((family.getValue() != null) && (family.getValue().size() > 0)) {
          if (family.getValue() instanceof Set) {
            // for each qualifier of the family
            Set<byte[]> familySet = (Set<byte[]>)family.getValue();
            for (byte[] qualifier : familySet) {
              if (!authManager.authorize(user, tableName, family.getKey(),
                                         qualifier, permRequest)) {
                return AuthResult.deny("Failed qualifier check", user,
                    permRequest, tableName, family.getKey(), qualifier);
              }
            }
          } else if (family.getValue() instanceof List) { // List<KeyValue>
            List<KeyValue> kvList = (List<KeyValue>)family.getValue();
            for (KeyValue kv : kvList) {
              if (!authManager.authorize(user, tableName, family.getKey(),
                      kv.getQualifier(), permRequest)) {
                return AuthResult.deny("Failed qualifier check", user,
                    permRequest, tableName, family.getKey(), kv.getQualifier());
              }
            }
          }
        } else {
          // no qualifiers and family-level check already failed
          return AuthResult.deny("Failed family check", user, permRequest,
              tableName, family.getKey(), null);
        }
      }

      // all family checks passed
      return AuthResult.allow("All family checks passed", user, permRequest,
          tableName);
    }

    // 5. no families to check and table level access failed
    return AuthResult.deny("No families to check and table permission failed",
        user, permRequest, tableName);
  }

  private void logResult(AuthResult result) {
    if (AUDITLOG.isTraceEnabled()) {
      AUDITLOG.trace("Access " + (result.isAllowed() ? "allowed" : "denied") +
          " for user " + (result.getUser() != null ? result.getUser().getShortName() : "UNKNOWN") +
          "; reason: " + result.getReason() +
          "; context: " + result.toContextString());
    }
  }

  /**
   * Returns the active user to which authorization checks should be applied.
   * If we are in the context of an RPC call, the remote user is used,
   * otherwise the currently logged in user is used.
   */
  private User getActiveUser() throws IOException {
    User user = RequestContext.getRequestUser();
    if (!RequestContext.isInRequestContext()) {
      // for non-rpc handling, fallback to system user
      user = User.getCurrent();
    }
    return user;
  }

  /**
   * Authorizes that the current user has global privileges for the given action.
   * @param perm The action being requested
   * @throws IOException if obtaining the current user fails
   * @throws AccessDeniedException if authorization is denied
   */
  private void requirePermission(Permission.Action perm) throws IOException {
    User user = getActiveUser();
    if (authManager.authorize(user, perm)) {
      logResult(AuthResult.allow("Global check allowed", user, perm, null));
    } else {
      logResult(AuthResult.deny("Global check failed", user, perm, null));
      throw new AccessDeniedException("Insufficient permissions for user '" +
          (user != null ? user.getShortName() : "null") +"' (global, action=" +
          perm.toString() + ")");
    }
  }

  /**
   * Authorizes that the current user has permission to perform the given
   * action on the set of table column families.
   * @param perm Action that is required
   * @param env The current coprocessor environment
   * @param families The set of column families present/required in the request
   * @throws AccessDeniedException if the authorization check failed
   */
  private void requirePermission(Permission.Action perm,
        RegionCoprocessorEnvironment env, Collection<byte[]> families)
      throws IOException {
    // create a map of family-qualifier
    HashMap<byte[], Set<byte[]>> familyMap = new HashMap<byte[], Set<byte[]>>();
    for (byte[] family : families) {
      familyMap.put(family, null);
    }
    requirePermission(perm, env, familyMap);
  }

  /**
   * Authorizes that the current user has permission to perform the given
   * action on the set of table column families.
   * @param perm Action that is required
   * @param env The current coprocessor environment
   * @param families The map of column families-qualifiers.
   * @throws AccessDeniedException if the authorization check failed
   */
  private void requirePermission(Permission.Action perm,
        RegionCoprocessorEnvironment env,
        Map<byte[], ? extends Collection<?>> families)
      throws IOException {
    User user = getActiveUser();
    AuthResult result = permissionGranted(user, perm, env, families);
    logResult(result);

    if (!result.isAllowed()) {
      StringBuffer sb = new StringBuffer("");
      if ((families != null && families.size() > 0)) {
        for (byte[] familyName : families.keySet()) {
          if (sb.length() != 0) {
            sb.append(", ");
          }
          sb.append(Bytes.toString(familyName));
        }
      }
      throw new AccessDeniedException("Insufficient permissions (table=" +
        env.getRegion().getTableDesc().getNameAsString()+
        ((families != null && families.size() > 0) ? ", family: " +
        sb.toString() : "") + ", action=" +
        perm.toString() + ")");
    }
  }

  /**
   * Returns <code>true</code> if the current user is allowed the given action
   * over at least one of the column qualifiers in the given column families.
   */
  private boolean hasFamilyQualifierPermission(User user,
      TablePermission.Action perm,
      RegionCoprocessorEnvironment env,
      Map<byte[], ? extends Set<byte[]>> familyMap)
    throws IOException {
    HRegionInfo hri = env.getRegion().getRegionInfo();
    byte[] tableName = hri.getTableName();

    if (user == null) {
      return false;
    }

    if (familyMap != null && familyMap.size() > 0) {
      // at least one family must be allowed
      for (Map.Entry<byte[], ? extends Set<byte[]>> family :
          familyMap.entrySet()) {
        if (family.getValue() != null && !family.getValue().isEmpty()) {
          for (byte[] qualifier : family.getValue()) {
            if (authManager.matchPermission(user, tableName,
                family.getKey(), qualifier, perm)) {
              return true;
            }
          }
        } else {
          if (authManager.matchPermission(user, tableName, family.getKey(),
              perm)) {
            return true;
          }
        }
      }
    } else if (LOG.isDebugEnabled()) {
      LOG.debug("Empty family map passed for permission check");
    }

    return false;
  }

  /* ---- MasterObserver implementation ---- */
  public void start(CoprocessorEnvironment env) throws IOException {
    // if running on HMaster
    if (env instanceof MasterCoprocessorEnvironment) {
      MasterCoprocessorEnvironment e = (MasterCoprocessorEnvironment)env;
      this.authManager = TableAuthManager.get(
          e.getMasterServices().getZooKeeper(),
          e.getConfiguration());
    }

    // if running at region
    if (env instanceof RegionCoprocessorEnvironment) {
      regionEnv = (RegionCoprocessorEnvironment)env;
    }
  }

  public void stop(CoprocessorEnvironment env) {

  }

  @Override
  public void preCreateTable(ObserverContext<MasterCoprocessorEnvironment> c,
      HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
    requirePermission(Permission.Action.CREATE);

    // default the table owner if not specified
    User owner = getActiveUser();
    if (desc.getOwnerString() == null ||
        desc.getOwnerString().equals("")) {
      desc.setOwner(owner);
    }
  }

  @Override
  public void postCreateTable(ObserverContext<MasterCoprocessorEnvironment> c,
      HTableDescriptor desc, HRegionInfo[] regions) throws IOException {}

  @Override
  public void preDeleteTable(ObserverContext<MasterCoprocessorEnvironment> c,
      byte[] tableName) throws IOException {
    requirePermission(Permission.Action.CREATE);
  }
  @Override
  public void postDeleteTable(ObserverContext<MasterCoprocessorEnvironment> c,
      byte[] tableName) throws IOException {}


  @Override
  public void preModifyTable(ObserverContext<MasterCoprocessorEnvironment> c,
      byte[] tableName, HTableDescriptor htd) throws IOException {
    requirePermission(Permission.Action.CREATE);
  }
  @Override
  public void postModifyTable(ObserverContext<MasterCoprocessorEnvironment> c,
      byte[] tableName, HTableDescriptor htd) throws IOException {}


  @Override
  public void preAddColumn(ObserverContext<MasterCoprocessorEnvironment> c,
      byte[] tableName, HColumnDescriptor column) throws IOException {
    requirePermission(Permission.Action.CREATE);
  }
  @Override
  public void postAddColumn(ObserverContext<MasterCoprocessorEnvironment> c,
      byte[] tableName, HColumnDescriptor column) throws IOException {}


  @Override
  public void preModifyColumn(ObserverContext<MasterCoprocessorEnvironment> c,
      byte[] tableName, HColumnDescriptor descriptor) throws IOException {
    requirePermission(Permission.Action.CREATE);
  }
  @Override
  public void postModifyColumn(ObserverContext<MasterCoprocessorEnvironment> c,
      byte[] tableName, HColumnDescriptor descriptor) throws IOException {}


  @Override
  public void preDeleteColumn(ObserverContext<MasterCoprocessorEnvironment> c,
      byte[] tableName, byte[] col) throws IOException {
    requirePermission(Permission.Action.CREATE);
  }
  @Override
  public void postDeleteColumn(ObserverContext<MasterCoprocessorEnvironment> c,
      byte[] tableName, byte[] col) throws IOException {}


  @Override
  public void preEnableTable(ObserverContext<MasterCoprocessorEnvironment> c,
      byte[] tableName) throws IOException {
    /* TODO: Allow for users with global CREATE permission and the table owner */
    requirePermission(Permission.Action.ADMIN);
  }
  @Override
  public void postEnableTable(ObserverContext<MasterCoprocessorEnvironment> c,
      byte[] tableName) throws IOException {}

  @Override
  public void preDisableTable(ObserverContext<MasterCoprocessorEnvironment> c,
      byte[] tableName) throws IOException {
    /* TODO: Allow for users with global CREATE permission and the table owner */
    requirePermission(Permission.Action.ADMIN);
  }
  @Override
  public void postDisableTable(ObserverContext<MasterCoprocessorEnvironment> c,
      byte[] tableName) throws IOException {}

  @Override
  public void preMove(ObserverContext<MasterCoprocessorEnvironment> c,
      HRegionInfo region, ServerName srcServer, ServerName destServer)
    throws IOException {
    requirePermission(Permission.Action.ADMIN);
  }
  @Override
  public void postMove(ObserverContext<MasterCoprocessorEnvironment> c,
      HRegionInfo region, ServerName srcServer, ServerName destServer)
    throws IOException {}

  @Override
  public void preAssign(ObserverContext<MasterCoprocessorEnvironment> c,
      HRegionInfo regionInfo) throws IOException {
    requirePermission(Permission.Action.ADMIN);
  }
  @Override
  public void postAssign(ObserverContext<MasterCoprocessorEnvironment> c,
      HRegionInfo regionInfo) throws IOException {}

  @Override
  public void preUnassign(ObserverContext<MasterCoprocessorEnvironment> c,
       HRegionInfo regionInfo, boolean force) throws IOException {
    requirePermission(Permission.Action.ADMIN);
  }
  @Override
  public void postUnassign(ObserverContext<MasterCoprocessorEnvironment> c,
      HRegionInfo regionInfo, boolean force) throws IOException {}

  @Override
  public void preBalance(ObserverContext<MasterCoprocessorEnvironment> c)
      throws IOException {
    requirePermission(Permission.Action.ADMIN);
  }
  @Override
  public void postBalance(ObserverContext<MasterCoprocessorEnvironment> c)
      throws IOException {}

  @Override
  public boolean preBalanceSwitch(ObserverContext<MasterCoprocessorEnvironment> c,
      boolean newValue) throws IOException {
    requirePermission(Permission.Action.ADMIN);
    return newValue;
  }
  @Override
  public void postBalanceSwitch(ObserverContext<MasterCoprocessorEnvironment> c,
      boolean oldValue, boolean newValue) throws IOException {}

  @Override
  public void preShutdown(ObserverContext<MasterCoprocessorEnvironment> c)
      throws IOException {
    requirePermission(Permission.Action.ADMIN);
  }

  @Override
  public void preStopMaster(ObserverContext<MasterCoprocessorEnvironment> c)
      throws IOException {
    requirePermission(Permission.Action.ADMIN);
  }

  @Override
  public void postStartMaster(ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException {
    // initialize the ACL storage table
    AccessControlLists.init(ctx.getEnvironment().getMasterServices());
  }


  /* ---- RegionObserver implementation ---- */

  @Override
  public void postOpen(ObserverContext<RegionCoprocessorEnvironment> c) {
    RegionCoprocessorEnvironment e = c.getEnvironment();
    final HRegion region = e.getRegion();
    if (region == null) {
      LOG.error("NULL region from RegionCoprocessorEnvironment in postOpen()");
      return;
    }

    try {
      this.authManager = TableAuthManager.get(
          e.getRegionServerServices().getZooKeeper(),
          e.getRegion().getConf());
    } catch (IOException ioe) {
      // pass along as a RuntimeException, so that the coprocessor is unloaded
      throw new RuntimeException("Error obtaining TableAuthManager", ioe);
    }

    if (AccessControlLists.isAclRegion(region)) {
      aclRegion = true;
      try {
        initialize(e);
      } catch (IOException ex) {
        // if we can't obtain permissions, it's better to fail
        // than perform checks incorrectly
        throw new RuntimeException("Failed to initialize permissions cache", ex);
      }
    }
  }

  @Override
  public void preGetClosestRowBefore(final ObserverContext<RegionCoprocessorEnvironment> c,
      final byte [] row, final byte [] family, final Result result)
      throws IOException {
    requirePermission(TablePermission.Action.READ, c.getEnvironment(),
        (family != null ? Lists.newArrayList(family) : null));
  }

  @Override
  public void preGet(final ObserverContext<RegionCoprocessorEnvironment> c,
      final Get get, final List<KeyValue> result) throws IOException {
    /*
     if column family level checks fail, check for a qualifier level permission
     in one of the families.  If it is present, then continue with the AccessControlFilter.
      */
    RegionCoprocessorEnvironment e = c.getEnvironment();
    User requestUser = getActiveUser();
    AuthResult authResult = permissionGranted(requestUser,
        TablePermission.Action.READ, e, get.getFamilyMap());
    if (!authResult.isAllowed()) {
      if (hasFamilyQualifierPermission(requestUser,
          TablePermission.Action.READ, e, get.getFamilyMap())) {
        byte[] table = getTableName(e);
        AccessControlFilter filter = new AccessControlFilter(authManager,
            requestUser, table);

        // wrap any existing filter
        if (get.getFilter() != null) {
          FilterList wrapper = new FilterList(FilterList.Operator.MUST_PASS_ALL,
              Lists.newArrayList(filter, get.getFilter()));
          get.setFilter(wrapper);
        } else {
          get.setFilter(filter);
        }
        logResult(AuthResult.allow("Access allowed with filter", requestUser,
            TablePermission.Action.READ, authResult.table));
      } else {
        logResult(authResult);
        throw new AccessDeniedException("Insufficient permissions (table=" +
          e.getRegion().getTableDesc().getNameAsString() + ", action=READ)");
      }
    } else {
      // log auth success
      logResult(authResult);
    }
  }

  @Override
  public boolean preExists(final ObserverContext<RegionCoprocessorEnvironment> c,
      final Get get, final boolean exists) throws IOException {
    requirePermission(TablePermission.Action.READ, c.getEnvironment(),
        get.familySet());
    return exists;
  }

  @Override
  public void prePut(final ObserverContext<RegionCoprocessorEnvironment> c,
      final Put put, final WALEdit edit, final boolean writeToWAL)
      throws IOException {
    requirePermission(TablePermission.Action.WRITE, c.getEnvironment(),
        put.getFamilyMap());
  }

  @Override
  public void postPut(final ObserverContext<RegionCoprocessorEnvironment> c,
      final Put put, final WALEdit edit, final boolean writeToWAL) {
    if (aclRegion) {
      updateACL(c.getEnvironment(), put.getFamilyMap());
    }
  }

  @Override
  public void preDelete(final ObserverContext<RegionCoprocessorEnvironment> c,
      final Delete delete, final WALEdit edit, final boolean writeToWAL)
      throws IOException {
    requirePermission(TablePermission.Action.WRITE, c.getEnvironment(),
        delete.getFamilyMap());
  }

  @Override
  public void postDelete(final ObserverContext<RegionCoprocessorEnvironment> c,
      final Delete delete, final WALEdit edit, final boolean writeToWAL)
      throws IOException {
    if (aclRegion) {
      updateACL(c.getEnvironment(), delete.getFamilyMap());
    }
  }

  @Override
  public boolean preCheckAndPut(final ObserverContext<RegionCoprocessorEnvironment> c,
      final byte [] row, final byte [] family, final byte [] qualifier,
      final CompareFilter.CompareOp compareOp,
      final WritableByteArrayComparable comparator, final Put put,
      final boolean result) throws IOException {
    requirePermission(TablePermission.Action.READ, c.getEnvironment(),
        Arrays.asList(new byte[][]{family}));
    return result;
  }

  @Override
  public boolean preCheckAndDelete(final ObserverContext<RegionCoprocessorEnvironment> c,
      final byte [] row, final byte [] family, final byte [] qualifier,
      final CompareFilter.CompareOp compareOp,
      final WritableByteArrayComparable comparator, final Delete delete,
      final boolean result) throws IOException {
    requirePermission(TablePermission.Action.READ, c.getEnvironment(),
        Arrays.asList( new byte[][] {family}));
    return result;
  }

  @Override
  public long preIncrementColumnValue(final ObserverContext<RegionCoprocessorEnvironment> c,
      final byte [] row, final byte [] family, final byte [] qualifier,
      final long amount, final boolean writeToWAL)
      throws IOException {
    requirePermission(TablePermission.Action.WRITE, c.getEnvironment(),
        Arrays.asList(new byte[][]{family}));
    return -1;
  }

  @Override
  public void preIncrement(final ObserverContext<RegionCoprocessorEnvironment> c,
      final Increment increment, final Result result)
      throws IOException {
    requirePermission(TablePermission.Action.WRITE, c.getEnvironment(),
        increment.getFamilyMap().keySet());
  }

  @Override
  public RegionScanner preScannerOpen(final ObserverContext<RegionCoprocessorEnvironment> c,
      final Scan scan, final RegionScanner s) throws IOException {
    /*
     if column family level checks fail, check for a qualifier level permission
     in one of the families.  If it is present, then continue with the AccessControlFilter.
      */
    RegionCoprocessorEnvironment e = c.getEnvironment();
    User user = getActiveUser();
    AuthResult authResult = permissionGranted(user, TablePermission.Action.READ, e,
        scan.getFamilyMap());
    if (!authResult.isAllowed()) {
      if (hasFamilyQualifierPermission(user, TablePermission.Action.READ, e,
          scan.getFamilyMap())) {
        byte[] table = getTableName(e);
        AccessControlFilter filter = new AccessControlFilter(authManager,
            user, table);

        // wrap any existing filter
        if (scan.hasFilter()) {
          FilterList wrapper = new FilterList(FilterList.Operator.MUST_PASS_ALL,
              Lists.newArrayList(filter, scan.getFilter()));
          scan.setFilter(wrapper);
        } else {
          scan.setFilter(filter);
        }
        logResult(AuthResult.allow("Access allowed with filter", user,
            TablePermission.Action.READ, authResult.table));
      } else {
        // no table/family level perms and no qualifier level perms, reject
        logResult(authResult);
        throw new AccessDeniedException("Insufficient permissions for user '"+
            (user != null ? user.getShortName() : "null")+"' "+
            "for scanner open on table " + Bytes.toString(getTableName(e)));
      }
    } else {
      // log success
      logResult(authResult);
    }
    return s;
  }

  @Override
  public RegionScanner postScannerOpen(final ObserverContext<RegionCoprocessorEnvironment> c,
      final Scan scan, final RegionScanner s) throws IOException {
    User user = getActiveUser();
    if (user != null && user.getShortName() != null) {      // store reference to scanner owner for later checks
      scannerOwners.put(s, user.getShortName());
    }
    return s;
  }

  @Override
  public boolean preScannerNext(final ObserverContext<RegionCoprocessorEnvironment> c,
      final InternalScanner s, final List<Result> result,
      final int limit, final boolean hasNext) throws IOException {
    requireScannerOwner(s);
    return hasNext;
  }

  @Override
  public void preScannerClose(final ObserverContext<RegionCoprocessorEnvironment> c,
      final InternalScanner s) throws IOException {
    requireScannerOwner(s);
  }

  @Override
  public void postScannerClose(final ObserverContext<RegionCoprocessorEnvironment> c,
      final InternalScanner s) throws IOException {
    // clean up any associated owner mapping
    scannerOwners.remove(s);
  }

  /**
   * Verify, when servicing an RPC, that the caller is the scanner owner.
   * If so, we assume that access control is correctly enforced based on
   * the checks performed in preScannerOpen()
   */
  private void requireScannerOwner(InternalScanner s)
      throws AccessDeniedException {
    if (RequestContext.isInRequestContext()) {
      String owner = scannerOwners.get(s);
      if (owner != null && !owner.equals(RequestContext.getRequestUserName())) {
        throw new AccessDeniedException("User '"+
            RequestContext.getRequestUserName()+"' is not the scanner owner!");
      }
    }
  }

  /* ---- AccessControllerProtocol implementation ---- */
  /*
   * These methods are only allowed to be called against the _acl_ region(s).
   * This will be restricted by both client side and endpoint implementations.
   */
  @Override
  public void grant(byte[] user, TablePermission permission)
      throws IOException {
    // verify it's only running at .acl.
    if (aclRegion) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Received request to grant access permission to '"
            + Bytes.toString(user) + "'. "
            + permission.toString());
      }

      requirePermission(Permission.Action.ADMIN);

      AccessControlLists.addTablePermission(regionEnv.getConfiguration(),
          permission.getTable(), Bytes.toString(user), permission);
      if (AUDITLOG.isTraceEnabled()) {
        // audit log should store permission changes in addition to auth results
        AUDITLOG.trace("Granted user '" + Bytes.toString(user) + "' permission "
            + permission.toString());
      }
    } else {
      throw new CoprocessorException(AccessController.class, "This method " +
          "can only execute at " +
          Bytes.toString(AccessControlLists.ACL_TABLE_NAME) + " table.");
    }
  }

  @Override
  public void revoke(byte[] user, TablePermission permission)
      throws IOException{
    // only allowed to be called on _acl_ region
    if (aclRegion) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Received request to revoke access permission for '"
            + Bytes.toString(user) + "'. "
            + permission.toString());
      }

      requirePermission(Permission.Action.ADMIN);

      AccessControlLists.removeTablePermission(regionEnv.getConfiguration(),
          permission.getTable(), Bytes.toString(user), permission);
      if (AUDITLOG.isTraceEnabled()) {
        // audit log should record all permission changes
        AUDITLOG.trace("Revoked user '" + Bytes.toString(user) + "' permission "
            + permission.toString());
      }
    } else {
      throw new CoprocessorException(AccessController.class, "This method " +
          "can only execute at " +
          Bytes.toString(AccessControlLists.ACL_TABLE_NAME) + " table.");
    }
  }

  @Override
  public List<UserPermission> getUserPermissions(final byte[] tableName)
      throws IOException {
    // only allowed to be called on _acl_ region
    if (aclRegion) {
      requirePermission(Permission.Action.ADMIN);

      List<UserPermission> perms = AccessControlLists.getUserPermissions
          (regionEnv.getConfiguration(), tableName);
      return perms;
    } else {
      throw new CoprocessorException(AccessController.class, "This method " +
          "can only execute at " +
          Bytes.toString(AccessControlLists.ACL_TABLE_NAME) + " table.");
    }
  }

  @Override
  public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
    return PROTOCOL_VERSION;
  }

  @Override
  public ProtocolSignature getProtocolSignature(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    if (AccessControllerProtocol.class.getName().equals(protocol)) {
      return new ProtocolSignature(PROTOCOL_VERSION, null);
    }
    throw new HBaseRPC.UnknownProtocolException(
        "Unexpected protocol requested: "+protocol);
  }

  private byte[] getTableName(RegionCoprocessorEnvironment e) {
    HRegion region = e.getRegion();
    byte[] tableName = null;

    if (region != null) {
      HRegionInfo regionInfo = region.getRegionInfo();
      if (regionInfo != null) {
        tableName = regionInfo.getTableName();
      }
    }
    return tableName;
  }
}
