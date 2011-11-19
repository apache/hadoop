/*
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

package org.apache.hadoop.hbase.security.access;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.Text;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.*;

/**
 * Maintains lists of permission grants to users and groups to allow for
 * authorization checks by {@link AccessController}.
 *
 * <p>
 * Access control lists are stored in an "internal" metadata table named
 * {@code _acl_}. Each table's permission grants are stored as a separate row,
 * keyed by the table name. KeyValues for permissions assignments are stored
 * in one of the formats:
 * <pre>
 * Key                      Desc
 * --------                 --------
 * user                     table level permissions for a user [R=read, W=write]
 * @group                   table level permissions for a group
 * user,family              column family level permissions for a user
 * @group,family            column family level permissions for a group
 * user,family,qualifier    column qualifier level permissions for a user
 * @group,family,qualifier  column qualifier level permissions for a group
 * </pre>
 * All values are encoded as byte arrays containing the codes from the
 * {@link org.apache.hadoop.hbase.security.access.TablePermission.Action} enum.
 * </p>
 */
public class AccessControlLists {
  /** Internal storage table for access control lists */
  public static final String ACL_TABLE_NAME_STR = "_acl_";
  public static final byte[] ACL_TABLE_NAME = Bytes.toBytes(ACL_TABLE_NAME_STR);
  /** Column family used to store ACL grants */
  public static final String ACL_LIST_FAMILY_STR = "l";
  public static final byte[] ACL_LIST_FAMILY = Bytes.toBytes(ACL_LIST_FAMILY_STR);

  /** Table descriptor for ACL internal table */
  public static final HTableDescriptor ACL_TABLEDESC = new HTableDescriptor(
      ACL_TABLE_NAME);
  static {
    ACL_TABLEDESC.addFamily(
        new HColumnDescriptor(ACL_LIST_FAMILY,
            10, // Ten is arbitrary number.  Keep versions to help debugging.
            Compression.Algorithm.NONE.getName(), true, true, 8 * 1024,
            HConstants.FOREVER, StoreFile.BloomType.NONE.toString(),
            HConstants.REPLICATION_SCOPE_LOCAL));
  }

  /**
   * Delimiter to separate user, column family, and qualifier in
   * _acl_ table info: column keys */
  public static final char ACL_KEY_DELIMITER = ',';
  /** Prefix character to denote group names */
  public static final String GROUP_PREFIX = "@";
  /** Configuration key for superusers */
  public static final String SUPERUSER_CONF_KEY = "hbase.superuser";

  private static Log LOG = LogFactory.getLog(AccessControlLists.class);

  /**
   * Check for existence of {@code _acl_} table and create it if it does not exist
   * @param master reference to HMaster
   */
  static void init(MasterServices master) throws IOException {
    if (!MetaReader.tableExists(master.getCatalogTracker(), ACL_TABLE_NAME_STR)) {
      master.createTable(ACL_TABLEDESC, null);
    }
  }

  /**
   * Stores a new table permission grant in the access control lists table.
   * @param conf the configuration
   * @param tableName the table to which access is being granted
   * @param username the user or group being granted the permission
   * @param perm the details of the permission being granted
   * @throws IOException in the case of an error accessing the metadata table
   */
  static void addTablePermission(Configuration conf,
      byte[] tableName, String username, TablePermission perm)
    throws IOException {

    Put p = new Put(tableName);
    byte[] key = Bytes.toBytes(username);
    if (perm.getFamily() != null && perm.getFamily().length > 0) {
      key = Bytes.add(key,
          Bytes.add(new byte[]{ACL_KEY_DELIMITER}, perm.getFamily()));
      if (perm.getQualifier() != null && perm.getQualifier().length > 0) {
        key = Bytes.add(key,
            Bytes.add(new byte[]{ACL_KEY_DELIMITER}, perm.getQualifier()));
      }
    }

    TablePermission.Action[] actions = perm.getActions();
    if ((actions == null) || (actions.length == 0)) {
      LOG.warn("No actions associated with user '"+username+"'");
      return;
    }

    byte[] value = new byte[actions.length];
    for (int i = 0; i < actions.length; i++) {
      value[i] = actions[i].code();
    }
    p.add(ACL_LIST_FAMILY, key, value);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Writing permission for table "+
          Bytes.toString(tableName)+" "+
          Bytes.toString(key)+": "+Bytes.toStringBinary(value)
      );
    }
    HTable acls = null;
    try {
      acls = new HTable(conf, ACL_TABLE_NAME);
      acls.put(p);
    } finally {
      if (acls != null) acls.close();
    }
  }

  /**
   * Removes a previously granted permission from the stored access control
   * lists.  The {@link TablePermission} being removed must exactly match what
   * is stored -- no wildcard matching is attempted.  Ie, if user "bob" has
   * been granted "READ" access to the "data" table, but only to column family
   * plus qualifier "info:colA", then trying to call this method with only
   * user "bob" and the table name "data" (but without specifying the
   * column qualifier "info:colA") will have no effect.
   *
   * @param conf the configuration
   * @param tableName the table of the current permission grant
   * @param userName the user or group currently granted the permission
   * @param perm the details of the permission to be revoked
   * @throws IOException if there is an error accessing the metadata table
   */
  static void removeTablePermission(Configuration conf,
      byte[] tableName, String userName, TablePermission perm)
    throws IOException {

    Delete d = new Delete(tableName);
    byte[] key = null;
    if (perm.getFamily() != null && perm.getFamily().length > 0) {
      key = Bytes.toBytes(userName + ACL_KEY_DELIMITER +
          Bytes.toString(perm.getFamily()));
      if (perm.getQualifier() != null && perm.getQualifier().length > 0) {
       key = Bytes.toBytes(userName + ACL_KEY_DELIMITER +
          Bytes.toString(perm.getFamily()) + ACL_KEY_DELIMITER +
          Bytes.toString(perm.getQualifier()));
      } else {
        key = Bytes.toBytes(userName + ACL_KEY_DELIMITER +
          Bytes.toString(perm.getFamily()));
      }
    } else {
      key = Bytes.toBytes(userName);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Removing permission for user '" + userName+ "': "+
          perm.toString());
    }
    d.deleteColumns(ACL_LIST_FAMILY, key);
    HTable acls = null;
    try {
      acls = new HTable(conf, ACL_TABLE_NAME);
      acls.delete(d);
    } finally {
      if (acls != null) acls.close();
    }
  }

  /**
   * Returns {@code true} if the given region is part of the {@code _acl_}
   * metadata table.
   */
  static boolean isAclRegion(HRegion region) {
    return Bytes.equals(ACL_TABLE_NAME, region.getTableDesc().getName());
  }

  /**
   * Loads all of the permission grants stored in a region of the {@code _acl_}
   * table.
   *
   * @param aclRegion
   * @return
   * @throws IOException
   */
  static Map<byte[],ListMultimap<String,TablePermission>> loadAll(
      HRegion aclRegion)
    throws IOException {

    if (!isAclRegion(aclRegion)) {
      throw new IOException("Can only load permissions from "+ACL_TABLE_NAME_STR);
    }

    Map<byte[],ListMultimap<String,TablePermission>> allPerms =
        new TreeMap<byte[],ListMultimap<String,TablePermission>>(Bytes.BYTES_COMPARATOR);
    
    // do a full scan of _acl_ table

    Scan scan = new Scan();
    scan.addFamily(ACL_LIST_FAMILY);

    InternalScanner iScanner = null;
    try {
      iScanner = aclRegion.getScanner(scan);

      while (true) {
        List<KeyValue> row = new ArrayList<KeyValue>();

        boolean hasNext = iScanner.next(row);
        ListMultimap<String,TablePermission> perms = ArrayListMultimap.create();
        byte[] table = null;
        for (KeyValue kv : row) {
          if (table == null) {
            table = kv.getRow();
          }
          Pair<String,TablePermission> permissionsOfUserOnTable =
              parseTablePermissionRecord(table, kv);
          if (permissionsOfUserOnTable != null) {
            String username = permissionsOfUserOnTable.getFirst();
            TablePermission permissions = permissionsOfUserOnTable.getSecond();
            perms.put(username, permissions);
          }
        }
        if (table != null) {
          allPerms.put(table, perms);
        }
        if (!hasNext) {
          break;
        }
      }
    } finally {
      if (iScanner != null) {
        iScanner.close();
      }
    }

    return allPerms;
  }

  /**
   * Load all permissions from the region server holding {@code _acl_},
   * primarily intended for testing purposes.
   */
  static Map<byte[],ListMultimap<String,TablePermission>> loadAll(
      Configuration conf) throws IOException {
    Map<byte[],ListMultimap<String,TablePermission>> allPerms =
        new TreeMap<byte[],ListMultimap<String,TablePermission>>(Bytes.BYTES_COMPARATOR);

    // do a full scan of _acl_, filtering on only first table region rows

    Scan scan = new Scan();
    scan.addFamily(ACL_LIST_FAMILY);

    HTable acls = null;
    ResultScanner scanner = null;
    try {
      acls = new HTable(conf, ACL_TABLE_NAME);
      scanner = acls.getScanner(scan);
      for (Result row : scanner) {
        ListMultimap<String,TablePermission> resultPerms =
            parseTablePermissions(row.getRow(), row);
        allPerms.put(row.getRow(), resultPerms);
      }
    } finally {
      if (scanner != null) scanner.close();
      if (acls != null) acls.close();
    }

    return allPerms;
  }

  /**
   * Reads user permission assignments stored in the <code>l:</code> column
   * family of the first table row in <code>_acl_</code>.
   *
   * <p>
   * See {@link AccessControlLists class documentation} for the key structure
   * used for storage.
   * </p>
   */
  static ListMultimap<String,TablePermission> getTablePermissions(
      Configuration conf, byte[] tableName)
  throws IOException {
    /* TODO: -ROOT- and .META. cannot easily be handled because they must be
     * online before _acl_ table.  Can anything be done here?
     */
    if (Bytes.equals(tableName, HConstants.ROOT_TABLE_NAME) ||
        Bytes.equals(tableName, HConstants.META_TABLE_NAME) ||
        Bytes.equals(tableName, AccessControlLists.ACL_TABLE_NAME)) {
      return ArrayListMultimap.create(0,0);
    }

    // for normal user tables, we just read the table row from _acl_
    ListMultimap<String,TablePermission> perms = ArrayListMultimap.create();
    HTable acls = null;
    try {
      acls = new HTable(conf, ACL_TABLE_NAME);
      Get get = new Get(tableName);
      get.addFamily(ACL_LIST_FAMILY);
      Result row = acls.get(get);
      if (!row.isEmpty()) {
        perms = parseTablePermissions(tableName, row);
      } else {
        LOG.info("No permissions found in "+ACL_TABLE_NAME_STR+
            " for table "+Bytes.toString(tableName));
      }
    } finally {
      if (acls != null) acls.close();
    }

    return perms;
  }

  /**
   * Returns the currently granted permissions for a given table as a list of
   * user plus associated permissions.
   */
  static List<UserPermission> getUserPermissions(
      Configuration conf, byte[] tableName)
  throws IOException {
    ListMultimap<String,TablePermission> allPerms = getTablePermissions(
      conf, tableName);

    List<UserPermission> perms = new ArrayList<UserPermission>();

    for (Map.Entry<String, TablePermission> entry : allPerms.entries()) {
      UserPermission up = new UserPermission(Bytes.toBytes(entry.getKey()),
          entry.getValue().getTable(), entry.getValue().getFamily(),
          entry.getValue().getQualifier(), entry.getValue().getActions());
      perms.add(up);
    }
    return perms;
  }

  private static ListMultimap<String,TablePermission> parseTablePermissions(
      byte[] table, Result result) {
    ListMultimap<String,TablePermission> perms = ArrayListMultimap.create();
    if (result != null && result.size() > 0) {
      for (KeyValue kv : result.raw()) {

        Pair<String,TablePermission> permissionsOfUserOnTable =
            parseTablePermissionRecord(table, kv);

        if (permissionsOfUserOnTable != null) {
          String username = permissionsOfUserOnTable.getFirst();
          TablePermission permissions = permissionsOfUserOnTable.getSecond();
          perms.put(username, permissions);
        }
      }
    }
    return perms;
  }

  private static Pair<String,TablePermission> parseTablePermissionRecord(
      byte[] table, KeyValue kv) {
    // return X given a set of permissions encoded in the permissionRecord kv.
    byte[] family = kv.getFamily();

    if (!Bytes.equals(family, ACL_LIST_FAMILY)) {
      return null;
    }

    byte[] key = kv.getQualifier();
    byte[] value = kv.getValue();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Read acl: kv ["+
                Bytes.toStringBinary(key)+": "+
                Bytes.toStringBinary(value)+"]");
    }

    // check for a column family appended to the key
    // TODO: avoid the string conversion to make this more efficient
    String username = Bytes.toString(key);
    int idx = username.indexOf(ACL_KEY_DELIMITER);
    byte[] permFamily = null;
    byte[] permQualifier = null;
    if (idx > 0 && idx < username.length()-1) {
      String remainder = username.substring(idx+1);
      username = username.substring(0, idx);
      idx = remainder.indexOf(ACL_KEY_DELIMITER);
      if (idx > 0 && idx < remainder.length()-1) {
        permFamily = Bytes.toBytes(remainder.substring(0, idx));
        permQualifier = Bytes.toBytes(remainder.substring(idx+1));
      } else {
        permFamily = Bytes.toBytes(remainder);
      }
    }

    return new Pair<String,TablePermission>(
        username, new TablePermission(table, permFamily, permQualifier, value));
  }

  /**
   * Writes a set of permissions as {@link org.apache.hadoop.io.Writable} instances
   * to the given output stream.
   * @param out
   * @param perms
   * @param conf
   * @throws IOException
   */
  public static void writePermissions(DataOutput out,
      ListMultimap<String,? extends Permission> perms, Configuration conf)
  throws IOException {
    Set<String> keys = perms.keySet();
    out.writeInt(keys.size());
    for (String key : keys) {
      Text.writeString(out, key);
      HbaseObjectWritable.writeObject(out, perms.get(key), List.class, conf);
    }
  }

  /**
   * Writes a set of permissions as {@link org.apache.hadoop.io.Writable} instances
   * and returns the resulting byte array.
   */
  public static byte[] writePermissionsAsBytes(
      ListMultimap<String,? extends Permission> perms, Configuration conf) {
    try {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      writePermissions(new DataOutputStream(bos), perms, conf);
      return bos.toByteArray();
    } catch (IOException ioe) {
      // shouldn't happen here
      LOG.error("Error serializing permissions", ioe);
    }
    return null;
  }

  /**
   * Reads a set of permissions as {@link org.apache.hadoop.io.Writable} instances
   * from the input stream.
   */
  public static <T extends Permission> ListMultimap<String,T> readPermissions(
      DataInput in, Configuration conf) throws IOException {
    ListMultimap<String,T> perms = ArrayListMultimap.create();
    int length = in.readInt();
    for (int i=0; i<length; i++) {
      String user = Text.readString(in);
      List<T> userPerms =
          (List)HbaseObjectWritable.readObject(in, conf);
      perms.putAll(user, userPerms);
    }

    return perms;
  }

  /**
   * Returns whether or not the given name should be interpreted as a group
   * principal.  Currently this simply checks if the name starts with the
   * special group prefix character ("@").
   */
  public static boolean isGroupPrincipal(String name) {
    return name != null && name.startsWith(GROUP_PREFIX);
  }

  /**
   * Returns the actual name for a group principal (stripped of the
   * group prefix).
   */
  public static String getGroupName(String aclKey) {
    if (!isGroupPrincipal(aclKey)) {
      return aclKey;
    }

    return aclKey.substring(GROUP_PREFIX.length());
  }
}
