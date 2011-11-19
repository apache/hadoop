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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Represents an authorization for access for the given actions, optionally
 * restricted to the given column family or column qualifier, over the
 * given table.  If the family property is <code>null</code>, it implies
 * full table access.
 */
public class TablePermission extends Permission {
  private static Log LOG = LogFactory.getLog(TablePermission.class);

  private byte[] table;
  private byte[] family;
  private byte[] qualifier;

  /** Nullary constructor for Writable, do not use */
  public TablePermission() {
    super();
  }

  /**
   * Create a new permission for the given table and (optionally) column family,
   * allowing the given actions.
   * @param table the table
   * @param family the family, can be null if a global permission on the table
   * @param assigned the list of allowed actions
   */
  public TablePermission(byte[] table, byte[] family, Action... assigned) {
    this(table, family, null, assigned);
  }

  /**
   * Creates a new permission for the given table, restricted to the given
   * column family and qualifer, allowing the assigned actions to be performed.
   * @param table the table
   * @param family the family, can be null if a global permission on the table
   * @param assigned the list of allowed actions
   */
  public TablePermission(byte[] table, byte[] family, byte[] qualifier,
      Action... assigned) {
    super(assigned);
    this.table = table;
    this.family = family;
    this.qualifier = qualifier;
  }

  /**
   * Creates a new permission for the given table, family and column qualifier,
   * allowing the actions matching the provided byte codes to be performed.
   * @param table the table
   * @param family the family, can be null if a global permission on the table
   * @param actionCodes the list of allowed action codes
   */
  public TablePermission(byte[] table, byte[] family, byte[] qualifier,
      byte[] actionCodes) {
    super(actionCodes);
    this.table = table;
    this.family = family;
    this.qualifier = qualifier;
  }

  public byte[] getTable() {
    return table;
  }

  public byte[] getFamily() {
    return family;
  }

  public byte[] getQualifier() {
    return qualifier;
  }

  /**
   * Checks that a given table operation is authorized by this permission
   * instance.
   *
   * @param table the table where the operation is being performed
   * @param family the column family to which the operation is restricted,
   *   if <code>null</code> implies "all"
   * @param qualifier the column qualifier to which the action is restricted,
   *   if <code>null</code> implies "all"
   * @param action the action being requested
   * @return <code>true</code> if the action within the given scope is allowed
   *   by this permission, <code>false</code>
   */
  public boolean implies(byte[] table, byte[] family, byte[] qualifier,
      Action action) {
    if (!Bytes.equals(this.table, table)) {
      return false;
    }

    if (this.family != null &&
        (family == null ||
         !Bytes.equals(this.family, family))) {
      return false;
    }

    if (this.qualifier != null &&
        (qualifier == null ||
         !Bytes.equals(this.qualifier, qualifier))) {
      return false;
    }

    // check actions
    return super.implies(action);
  }

  /**
   * Checks if this permission grants access to perform the given action on
   * the given table and key value.
   * @param table the table on which the operation is being performed
   * @param kv the KeyValue on which the operation is being requested
   * @param action the action requested
   * @return <code>true</code> if the action is allowed over the given scope
   *   by this permission, otherwise <code>false</code>
   */
  public boolean implies(byte[] table, KeyValue kv, Action action) {
    if (!Bytes.equals(this.table, table)) {
      return false;
    }

    if (family != null &&
        (Bytes.compareTo(family, 0, family.length,
            kv.getBuffer(), kv.getFamilyOffset(), kv.getFamilyLength()) != 0)) {
      return false;
    }

    if (qualifier != null &&
        (Bytes.compareTo(qualifier, 0, qualifier.length,
            kv.getBuffer(), kv.getQualifierOffset(), kv.getQualifierLength()) != 0)) {
      return false;
    }

    // check actions
    return super.implies(action);
  }

  /**
   * Returns <code>true</code> if this permission matches the given column
   * family at least.  This only indicates a partial match against the table
   * and column family, however, and does not guarantee that implies() for the
   * column same family would return <code>true</code>.  In the case of a
   * column-qualifier specific permission, for example, implies() would still
   * return false.
   */
  public boolean matchesFamily(byte[] table, byte[] family, Action action) {
    if (!Bytes.equals(this.table, table)) {
      return false;
    }

    if (this.family != null &&
        (family == null ||
         !Bytes.equals(this.family, family))) {
      return false;
    }

    // ignore qualifier
    // check actions
    return super.implies(action);
  }

  /**
   * Returns if the given permission matches the given qualifier.
   * @param table the table name to match
   * @param family the column family to match
   * @param qualifier the qualifier name to match
   * @param action the action requested
   * @return <code>true</code> if the table, family and qualifier match,
   *   otherwise <code>false</code>
   */
  public boolean matchesFamilyQualifier(byte[] table, byte[] family, byte[] qualifier,
                                Action action) {
    if (!matchesFamily(table, family, action)) {
      return false;
    } else {
      if (this.qualifier != null &&
          (qualifier == null ||
           !Bytes.equals(this.qualifier, qualifier))) {
        return false;
      }
    }
    return super.implies(action);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof TablePermission)) {
      return false;
    }
    TablePermission other = (TablePermission)obj;

    if (!(Bytes.equals(table, other.getTable()) &&
        ((family == null && other.getFamily() == null) ||
         Bytes.equals(family, other.getFamily())) &&
        ((qualifier == null && other.getQualifier() == null) ||
         Bytes.equals(qualifier, other.getQualifier()))
       )) {
      return false;
    }

    // check actions
    return super.equals(other);
  }

  @Override
  public int hashCode() {
    final int prime = 37;
    int result = super.hashCode();
    if (table != null) {
      result = prime * result + Bytes.hashCode(table);
    }
    if (family != null) {
      result = prime * result + Bytes.hashCode(family);
    }
    if (qualifier != null) {
      result = prime * result + Bytes.hashCode(qualifier);
    }
    return result;
  }

  public String toString() {
    StringBuilder str = new StringBuilder("[TablePermission: ")
        .append("table=").append(Bytes.toString(table))
        .append(", family=").append(Bytes.toString(family))
        .append(", qualifier=").append(Bytes.toString(qualifier))
        .append(", actions=");
    if (actions != null) {
      for (int i=0; i<actions.length; i++) {
        if (i > 0)
          str.append(",");
        if (actions[i] != null)
          str.append(actions[i].toString());
        else
          str.append("NULL");
      }
    }
    str.append("]");

    return str.toString();
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    table = Bytes.readByteArray(in);
    if (in.readBoolean()) {
      family = Bytes.readByteArray(in);
    }
    if (in.readBoolean()) {
      qualifier = Bytes.readByteArray(in);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    Bytes.writeByteArray(out, table);
    out.writeBoolean(family != null);
    if (family != null) {
      Bytes.writeByteArray(out, family);
    }
    out.writeBoolean(qualifier != null);
    if (qualifier != null) {
      Bytes.writeByteArray(out, qualifier);
    }
  }
}
