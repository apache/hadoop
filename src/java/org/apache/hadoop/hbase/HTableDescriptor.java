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
package org.apache.hadoop.hbase;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * HTableDescriptor contains the name of an HTable, and its
 * column families.
 */
public class HTableDescriptor implements WritableComparable {
  /** table descriptor for root table */
  public static final HTableDescriptor rootTableDesc =
    new HTableDescriptor(HConstants.ROOT_TABLE_NAME,
        new HColumnDescriptor(HConstants.COLUMN_FAMILY, 1,
            HColumnDescriptor.CompressionType.NONE, false, Integer.MAX_VALUE,
            null));
  
  /** table descriptor for meta table */
  public static final HTableDescriptor metaTableDesc =
    new HTableDescriptor(HConstants.META_TABLE_NAME,
        new HColumnDescriptor(HConstants.COLUMN_FAMILY, 1,
            HColumnDescriptor.CompressionType.NONE, false, Integer.MAX_VALUE,
            null));
  
  private boolean rootregion;
  private boolean metaregion;
  private Text name;
  // TODO: Does this need to be a treemap?  Can it be a HashMap?
  private final TreeMap<Text, HColumnDescriptor> families;
  
  /*
   * Legal table names can only contain 'word characters':
   * i.e. <code>[a-zA-Z_0-9-.]</code>.
   * Lets be restrictive until a reason to be otherwise. One reason to limit
   * characters in table name is to ensure table regions as entries in META
   * regions can be found (See HADOOP-1581 'HBASE: Un-openable tablename bug').
   */
  private static final Pattern LEGAL_TABLE_NAME =
    Pattern.compile("^[\\w-.]+$");

  /** Used to construct the table descriptors for root and meta tables */
  private HTableDescriptor(Text name, HColumnDescriptor family) {
    rootregion = name.equals(HConstants.ROOT_TABLE_NAME);
    this.metaregion = true;
    this.name = new Text(name);
    this.families = new TreeMap<Text, HColumnDescriptor>();
    families.put(family.getName(), family);
  }

  /**
   * Constructs an empty object.
   * For deserializing an HTableDescriptor instance only.
   * @see #HTableDescriptor(String)
   */
  public HTableDescriptor() {
    this.name = new Text();
    this.families = new TreeMap<Text, HColumnDescriptor>();
  }

  /**
   * Constructor.
   * @param name Table name.
   * @throws IllegalArgumentException if passed a table name
   * that is made of other than 'word' characters: i.e.
   * <code>[a-zA-Z_0-9]
   */
  public HTableDescriptor(String name) {
    this();
    Matcher m = LEGAL_TABLE_NAME.matcher(name);
    if (m == null || !m.matches()) {
      throw new IllegalArgumentException(
          "Table names can only contain 'word characters': i.e. [a-zA-Z_0-9");
    }
    this.name.set(name);
    this.rootregion = false;
    this.metaregion = false;
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
  public Text getName() {
    return name;
  }

  /**
   * Adds a column family.
   * @param family HColumnDescriptor of familyto add.
   */
  public void addFamily(HColumnDescriptor family) {
    if (family.getName() == null || family.getName().getLength() <= 0) {
      throw new NullPointerException("Family name cannot be null or empty");
    }
    families.put(family.getName(), family);
  }

  /**
   * Checks to see if this table contains the given column family
   * 
   * @param family - family name
   * @return true if the table contains the specified family name
   */
  public boolean hasFamily(Text family) {
    return families.containsKey(family);
  }

  /** 
   * All the column families in this table.
   * 
   *  TODO: What is this used for? Seems Dangerous to let people play with our
   *  private members.
   *  
   *  @return map of family members
   */
  public TreeMap<Text, HColumnDescriptor> families() {
    return families;
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return "name: " + this.name.toString() + ", families: " + this.families;
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
    int result = this.name.hashCode();
    if (this.families != null && this.families.size() > 0) {
      for (Map.Entry<Text,HColumnDescriptor> e: this.families.entrySet()) {
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
    name.write(out);
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
    this.name.readFields(in);
    int numCols = in.readInt();
    families.clear();
    for(int i = 0; i < numCols; i++) {
      HColumnDescriptor c = new HColumnDescriptor();
      c.readFields(in);
      families.put(c.getName(), c);
    }
  }

  // Comparable

  /** {@inheritDoc} */
  public int compareTo(Object o) {
    HTableDescriptor other = (HTableDescriptor) o;
    int result = name.compareTo(other.name);
    
    if(result == 0) {
      result = families.size() - other.families.size();
    }
    
    if(result == 0 && families.size() != other.families.size()) {
      result = Integer.valueOf(families.size()).compareTo(
          Integer.valueOf(other.families.size()));
    }
    
    if(result == 0) {
      for(Iterator<HColumnDescriptor> it = families.values().iterator(),
          it2 = other.families.values().iterator(); it.hasNext(); ) {
        result = it.next().compareTo(it2.next());
        if(result != 0) {
          break;
        }
      }
    }
    return result;
  }

  /**
   * @return Immutable sorted map of families.
   */
  public SortedMap<Text, HColumnDescriptor> getFamilies() {
    return Collections.unmodifiableSortedMap(this.families);
  }

  /**
   * @param rootdir qualified path of HBase root directory
   * @param tableName name of table
   * @return path for table
   */
  public static Path getTableDir(Path rootdir, Text tableName) {
    return new Path(rootdir, tableName.toString());
  }
}
