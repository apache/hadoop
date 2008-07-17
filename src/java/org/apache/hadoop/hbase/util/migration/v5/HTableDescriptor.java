/**
 * Copyright 2007 The Apache Software Foundation
 *
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

package org.apache.hadoop.hbase.util.migration.v5;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HStoreKey;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableComparable;

/**
 * HTableDescriptor contains the name of an HTable, and its
 * column families.
 */
public class HTableDescriptor implements WritableComparable {
  /** Table descriptor for <core>-ROOT-</code> catalog table */
  public static final HTableDescriptor ROOT_TABLEDESC = new HTableDescriptor(
      HConstants.ROOT_TABLE_NAME,
      new HColumnDescriptor[] { new HColumnDescriptor(HConstants.COLUMN_FAMILY,
          1, HColumnDescriptor.CompressionType.NONE, false, false,
          Integer.MAX_VALUE, HConstants.FOREVER, false) });
  
  /** Table descriptor for <code>.META.</code> catalog table */
  public static final HTableDescriptor META_TABLEDESC = new HTableDescriptor(
      HConstants.META_TABLE_NAME, new HColumnDescriptor[] {
          new HColumnDescriptor(HConstants.COLUMN_FAMILY, 1,
              HColumnDescriptor.CompressionType.NONE, false, false,
              Integer.MAX_VALUE, HConstants.FOREVER, false),
          new HColumnDescriptor(HConstants.COLUMN_FAMILY_HISTORIAN,
              HConstants.ALL_VERSIONS, HColumnDescriptor.CompressionType.NONE,
              false, false, Integer.MAX_VALUE, HConstants.FOREVER, false) });
  
  private boolean rootregion = false;
  private boolean metaregion = false;
  private byte [] name = HConstants.EMPTY_BYTE_ARRAY;
  private String nameAsString = "";
  
  public static final String FAMILIES = "FAMILIES";
  
  // Key is hash of the family name.
  private final Map<Integer, HColumnDescriptor> families =
    new HashMap<Integer, HColumnDescriptor>();

  /**
   * Private constructor used internally creating table descriptors for 
   * catalog tables: e.g. .META. and -ROOT-.
   */
  private HTableDescriptor(final byte [] name, HColumnDescriptor[] families) {
    this.name = name.clone();
    setMetaFlags(name);
    for(HColumnDescriptor descriptor : families) {
      this.families.put(Bytes.mapKey(descriptor.getName()), descriptor);
    }
  }

  /**
   * Constructs an empty object.
   * For deserializing an HTableDescriptor instance only.
   * @see #HTableDescriptor(byte[])
   */
  public HTableDescriptor() {
    super();
  }

  /**
   * Constructor.
   * @param name Table name.
   * @throws IllegalArgumentException if passed a table name
   * that is made of other than 'word' characters, underscore or period: i.e.
   * <code>[a-zA-Z_0-9.].
   * @see <a href="HADOOP-1581">HADOOP-1581 HBASE: Un-openable tablename bug</a>
   */
  public HTableDescriptor(final String name) {
    this(Bytes.toBytes(name));
  }

  /**
   * Constructor.
   * @param name Table name.
   * @throws IllegalArgumentException if passed a table name
   * that is made of other than 'word' characters, underscore or period: i.e.
   * <code>[a-zA-Z_0-9.].
   * @see <a href="HADOOP-1581">HADOOP-1581 HBASE: Un-openable tablename bug</a>
   */
  public HTableDescriptor(final byte [] name) {
    setMetaFlags(name);
    this.name = this.metaregion? name: isLegalTableName(name);
    this.nameAsString = Bytes.toString(this.name);
  }

  /*
   * Set meta flags on this table.
   * Called by constructors.
   * @param name
   */
  private void setMetaFlags(final byte [] name) {
    this.rootregion = Bytes.equals(name, HConstants.ROOT_TABLE_NAME);
    this.metaregion =
      this.rootregion? true: Bytes.equals(name, HConstants.META_TABLE_NAME);
  }
  
  /**
   * Check passed buffer is legal user-space table name.
   * @param b Table name.
   * @return Returns passed <code>b</code> param
   * @throws NullPointerException If passed <code>b</code> is null
   * @throws IllegalArgumentException if passed a table name
   * that is made of other than 'word' characters or underscores: i.e.
   * <code>[a-zA-Z_0-9].
   */
  public static byte [] isLegalTableName(final byte [] b) {
    if (b == null || b.length <= 0) {
      throw new IllegalArgumentException("Name is null or empty");
    }
    for (int i = 0; i < b.length; i++) {
      if (Character.isLetterOrDigit(b[i]) || b[i] == '_') {
        continue;
      }
      throw new IllegalArgumentException("Illegal character <" + b[i] + ">. " +
        "User-space table names can only contain 'word characters':" +
        "i.e. [a-zA-Z_0-9]: " + Bytes.toString(b));
    }
    return b;
  }

  /** @return true if this is the root region */
  public boolean isRootRegion() {
    return rootregion;
  }
  
  /** @return true if table is the meta table */
  public boolean isMetaTable() {
    return metaregion && !rootregion;
  }
  
  /** @return true if this is a meta region (part of the root or meta tables) */
  public boolean isMetaRegion() {
    return metaregion;
  }

  /** @return name of table */
  public byte [] getName() {
    return name;
  }

  /** @return name of table */
  public String getNameAsString() {
    return this.nameAsString;
  }

  /**
   * Adds a column family.
   * @param family HColumnDescriptor of familyto add.
   */
  public void addFamily(final HColumnDescriptor family) {
    if (family.getName() == null || family.getName().length <= 0) {
      throw new NullPointerException("Family name cannot be null or empty");
    }
    this.families.put(Bytes.mapKey(family.getName()), family);
  }

  /**
   * Checks to see if this table contains the given column family
   * @param c Family name or column name.
   * @return true if the table contains the specified family name
   */
  public boolean hasFamily(final byte [] c) {
    return hasFamily(c, HStoreKey.getFamilyDelimiterIndex(c));
  }

  /**
   * Checks to see if this table contains the given column family
   * @param c Family name or column name.
   * @param index Index to column family delimiter
   * @return true if the table contains the specified family name
   */
  public boolean hasFamily(final byte [] c, final int index) {
    // If index is -1, then presume we were passed a column family name minus
    // the colon delimiter.
    return families.containsKey(Bytes.mapKey(c, index == -1? c.length: index));
  }

  /**
   * @return Name of this table and then a map of all of the column family
   * descriptors.
   * @see #getNameAsString()
   */
  @Override
  public String toString() {
    return HConstants.NAME + " => '" + Bytes.toString(this.name) +
      "', " + FAMILIES + " => " + this.families.values();
  }
  
  /** {@inheritDoc} */
  @Override
  public boolean equals(Object obj) {
    return compareTo(obj) == 0;
  }
  
  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    // TODO: Cache.
    int result = Bytes.hashCode(this.name);
    if (this.families != null && this.families.size() > 0) {
      for (HColumnDescriptor e: this.families.values()) {
        result ^= e.hashCode();
      }
    }
    return result;
  }
  
  // Writable

  /** {@inheritDoc} */
  public void write(DataOutput out) throws IOException {
    out.writeBoolean(rootregion);
    out.writeBoolean(metaregion);
    Bytes.writeByteArray(out, name);
    out.writeInt(families.size());
    for(Iterator<HColumnDescriptor> it = families.values().iterator();
        it.hasNext(); ) {
      it.next().write(out);
    }
  }

  /** {@inheritDoc} */
  public void readFields(DataInput in) throws IOException {
    this.rootregion = in.readBoolean();
    this.metaregion = in.readBoolean();
    this.name = Bytes.readByteArray(in);
    this.nameAsString = Bytes.toString(this.name);
    int numCols = in.readInt();
    this.families.clear();
    for (int i = 0; i < numCols; i++) {
      HColumnDescriptor c = new HColumnDescriptor();
      c.readFields(in);
      this.families.put(Bytes.mapKey(c.getName()), c);
    }
  }

  // Comparable

  /** {@inheritDoc} */
  public int compareTo(Object o) {
    HTableDescriptor other = (HTableDescriptor) o;
    int result = Bytes.compareTo(this.name, other.name);
    if (result == 0) {
      result = families.size() - other.families.size();
    }
    
    if (result == 0 && families.size() != other.families.size()) {
      result = Integer.valueOf(families.size()).compareTo(
          Integer.valueOf(other.families.size()));
    }
    
    if (result == 0) {
      for (Iterator<HColumnDescriptor> it = families.values().iterator(),
          it2 = other.families.values().iterator(); it.hasNext(); ) {
        result = it.next().compareTo(it2.next());
        if (result != 0) {
          break;
        }
      }
    }
    return result;
  }

  /**
   * @return Immutable sorted map of families.
   */
  public Collection<HColumnDescriptor> getFamilies() {
    return Collections.unmodifiableCollection(this.families.values());
  }

  /**
   * @param column
   * @return Column descriptor for the passed family name or the family on
   * passed in column.
   */
  public HColumnDescriptor getFamily(final byte [] column) {
    return this.families.get(HStoreKey.getFamilyMapKey(column));
  }

  /**
   * @param column
   * @return Column descriptor for the passed family name or the family on
   * passed in column.
   */
  public HColumnDescriptor removeFamily(final byte [] column) {
    return this.families.remove(HStoreKey.getFamilyMapKey(column));
  }

  /**
   * @param rootdir qualified path of HBase root directory
   * @param tableName name of table
   * @return path for table
   */
  public static Path getTableDir(Path rootdir, final byte [] tableName) {
    return new Path(rootdir, Bytes.toString(tableName));
  }
}
