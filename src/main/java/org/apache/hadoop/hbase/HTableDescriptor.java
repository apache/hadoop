/**
 * Copyright 2009 The Apache Software Foundation
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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableComparable;

/**
 * HTableDescriptor contains the details about an HBase table  such as the descriptors of
 * all the column families, is the table a catalog table, <code> -ROOT- </code> or 
 * <code> .META. </code>, is the table is read only, the maximum size of the memstore, 
 * when the region split should occur, coprocessors associated with it etc...
 */
public class HTableDescriptor implements WritableComparable<HTableDescriptor> {

  /**
   *  Changes prior to version 3 were not recorded here.
   *  Version 3 adds metadata as a map where keys and values are byte[].
   *  Version 4 adds indexes
   *  Version 5 removed transactional pollution -- e.g. indexes
   */
  private static final byte TABLE_DESCRIPTOR_VERSION = 5;

  private byte [] name = HConstants.EMPTY_BYTE_ARRAY;

  private String nameAsString = "";

  /**
   * A map which holds the metadata information of the table. This metadata 
   * includes values like IS_ROOT, IS_META, DEFERRED_LOG_FLUSH, SPLIT_POLICY,
   * MAX_FILE_SIZE, READONLY, MEMSTORE_FLUSHSIZE etc...
   */
  protected Map<ImmutableBytesWritable, ImmutableBytesWritable> values =
    new HashMap<ImmutableBytesWritable, ImmutableBytesWritable>();

  private static final String FAMILIES = "FAMILIES";

  private static final String SPLIT_POLICY = "SPLIT_POLICY";
  
  /**
   * <em>INTERNAL</em> Used by HBase Shell interface to access this metadata 
   * attribute which denotes the maximum size of the store file after which 
   * a region split occurs
   * 
   * @see #getMaxFileSize()
   */
  public static final String MAX_FILESIZE = "MAX_FILESIZE";
  private static final ImmutableBytesWritable MAX_FILESIZE_KEY =
    new ImmutableBytesWritable(Bytes.toBytes(MAX_FILESIZE));

  public static final String OWNER = "OWNER";
  public static final ImmutableBytesWritable OWNER_KEY =
    new ImmutableBytesWritable(Bytes.toBytes(OWNER));

  /**
   * <em>INTERNAL</em> Used by rest interface to access this metadata 
   * attribute which denotes if the table is Read Only
   * 
   * @see #isReadOnly()
   */
  public static final String READONLY = "READONLY";
  private static final ImmutableBytesWritable READONLY_KEY =
    new ImmutableBytesWritable(Bytes.toBytes(READONLY));

  /**
   * <em>INTERNAL</em> Used by HBase Shell interface to access this metadata 
   * attribute which represents the maximum size of the memstore after which 
   * its contents are flushed onto the disk
   * 
   * @see #getMemStoreFlushSize()
   */
  public static final String MEMSTORE_FLUSHSIZE = "MEMSTORE_FLUSHSIZE";
  private static final ImmutableBytesWritable MEMSTORE_FLUSHSIZE_KEY =
    new ImmutableBytesWritable(Bytes.toBytes(MEMSTORE_FLUSHSIZE));

  /**
   * <em>INTERNAL</em> Used by rest interface to access this metadata 
   * attribute which denotes if the table is a -ROOT- region or not
   * 
   * @see #isRootRegion()
   */
  public static final String IS_ROOT = "IS_ROOT";
  private static final ImmutableBytesWritable IS_ROOT_KEY =
    new ImmutableBytesWritable(Bytes.toBytes(IS_ROOT));

  /**
   * <em>INTERNAL</em> Used by rest interface to access this metadata 
   * attribute which denotes if it is a catalog table, either
   * <code> .META. </code> or <code> -ROOT- </code>
   * 
   * @see #isMetaRegion()
   */
  public static final String IS_META = "IS_META";
  private static final ImmutableBytesWritable IS_META_KEY =
    new ImmutableBytesWritable(Bytes.toBytes(IS_META));

  /**
   * <em>INTERNAL</em> Used by HBase Shell interface to access this metadata 
   * attribute which denotes if the deferred log flush option is enabled
   */
  public static final String DEFERRED_LOG_FLUSH = "DEFERRED_LOG_FLUSH";
  private static final ImmutableBytesWritable DEFERRED_LOG_FLUSH_KEY =
    new ImmutableBytesWritable(Bytes.toBytes(DEFERRED_LOG_FLUSH));

  /*
   *  The below are ugly but better than creating them each time till we
   *  replace booleans being saved as Strings with plain booleans.  Need a
   *  migration script to do this.  TODO.
   */
  private static final ImmutableBytesWritable FALSE =
    new ImmutableBytesWritable(Bytes.toBytes(Boolean.FALSE.toString()));

  private static final ImmutableBytesWritable TRUE =
    new ImmutableBytesWritable(Bytes.toBytes(Boolean.TRUE.toString()));

  private static final boolean DEFAULT_DEFERRED_LOG_FLUSH = false;
  
  /**
   * Constant that denotes whether the table is READONLY by default and is false
   */
  public static final boolean DEFAULT_READONLY = false;

  /**
   * Constant that denotes the maximum default size of the memstore after which 
   * the contents are flushed to the store files
   */
  public static final long DEFAULT_MEMSTORE_FLUSH_SIZE = 1024*1024*64L;

  private volatile Boolean meta = null;
  private volatile Boolean root = null;
  private Boolean isDeferredLog = null;

  /**
   * Maps column family name to the respective HColumnDescriptors
   */
  private final Map<byte [], HColumnDescriptor> families =
    new TreeMap<byte [], HColumnDescriptor>(Bytes.BYTES_RAWCOMPARATOR);

  /**
   * <em> INTERNAL </em> Private constructor used internally creating table descriptors for
   * catalog tables, <code>.META.</code> and <code>-ROOT-</code>.
   */
  protected HTableDescriptor(final byte [] name, HColumnDescriptor[] families) {
    this.name = name.clone();
    this.nameAsString = Bytes.toString(this.name);
    setMetaFlags(name);
    for(HColumnDescriptor descriptor : families) {
      this.families.put(descriptor.getName(), descriptor);
    }
  }

  /**
   * <em> INTERNAL </em>Private constructor used internally creating table descriptors for
   * catalog tables, <code>.META.</code> and <code>-ROOT-</code>.
   */
  protected HTableDescriptor(final byte [] name, HColumnDescriptor[] families,
      Map<ImmutableBytesWritable,ImmutableBytesWritable> values) {
    this.name = name.clone();
    this.nameAsString = Bytes.toString(this.name);
    setMetaFlags(name);
    for(HColumnDescriptor descriptor : families) {
      this.families.put(descriptor.getName(), descriptor);
    }
    for (Map.Entry<ImmutableBytesWritable, ImmutableBytesWritable> entry:
        values.entrySet()) {
      this.values.put(entry.getKey(), entry.getValue());
    }
  }

  /**
   * Default constructor which constructs an empty object.
   * For deserializing an HTableDescriptor instance only.
   * @see #HTableDescriptor(byte[])
   */
  public HTableDescriptor() {
    super();
  }

  /**
   * Construct a table descriptor specifying table name.
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
   * Construct a table descriptor specifying a byte array table name
   * @param name - Table name as a byte array.
   * @throws IllegalArgumentException if passed a table name
   * that is made of other than 'word' characters, underscore or period: i.e.
   * <code>[a-zA-Z_0-9-.].
   * @see <a href="HADOOP-1581">HADOOP-1581 HBASE: Un-openable tablename bug</a>
   */
  public HTableDescriptor(final byte [] name) {
    super();
    setMetaFlags(this.name);
    this.name = this.isMetaRegion()? name: isLegalTableName(name);
    this.nameAsString = Bytes.toString(this.name);
  }

  /**
   * Construct a table descriptor by cloning the descriptor passed as a parameter.
   * <p>
   * Makes a deep copy of the supplied descriptor.
   * Can make a modifiable descriptor from an UnmodifyableHTableDescriptor.
   * @param desc The descriptor.
   */
  public HTableDescriptor(final HTableDescriptor desc) {
    super();
    this.name = desc.name.clone();
    this.nameAsString = Bytes.toString(this.name);
    setMetaFlags(this.name);
    for (HColumnDescriptor c: desc.families.values()) {
      this.families.put(c.getName(), new HColumnDescriptor(c));
    }
    for (Map.Entry<ImmutableBytesWritable, ImmutableBytesWritable> e:
        desc.values.entrySet()) {
      this.values.put(e.getKey(), e.getValue());
    }
  }

  /*
   * Set meta flags on this table. 
   * IS_ROOT_KEY is set if its a -ROOT- table
   * IS_META_KEY is set either if its a -ROOT- or a .META. table 
   * Called by constructors.
   * @param name
   */
  private void setMetaFlags(final byte [] name) {
    setRootRegion(Bytes.equals(name, HConstants.ROOT_TABLE_NAME));
    setMetaRegion(isRootRegion() ||
      Bytes.equals(name, HConstants.META_TABLE_NAME));
  }

  /**
   * Check if the descriptor represents a <code> -ROOT- </code> region.
   * 
   * @return true if this is a <code> -ROOT- </code> region 
   */
  public boolean isRootRegion() {
    if (this.root == null) {
      this.root = isSomething(IS_ROOT_KEY, false)? Boolean.TRUE: Boolean.FALSE;
    }
    return this.root.booleanValue();
  }

  /**
   * <em> INTERNAL </em> Used to denote if the current table represents 
   * <code> -ROOT- </code> region. This is used internally by the 
   * HTableDescriptor constructors 
   * 
   * @param isRoot true if this is the <code> -ROOT- </code> region 
   */
  protected void setRootRegion(boolean isRoot) {
    // TODO: Make the value a boolean rather than String of boolean.
    values.put(IS_ROOT_KEY, isRoot? TRUE: FALSE);
  }

  /**
   * Checks if this table is either <code> -ROOT- </code> or <code> .META. </code>
   * region. 
   *  
   * @return true if this is either a <code> -ROOT- </code> or <code> .META. </code> 
   * region 
   */
  public boolean isMetaRegion() {
    if (this.meta == null) {
      this.meta = calculateIsMetaRegion();
    }
    return this.meta.booleanValue();
  }

  private synchronized Boolean calculateIsMetaRegion() {
    byte [] value = getValue(IS_META_KEY);
    return (value != null)? Boolean.valueOf(Bytes.toString(value)): Boolean.FALSE;
  }

  private boolean isSomething(final ImmutableBytesWritable key,
      final boolean valueIfNull) {
    byte [] value = getValue(key);
    if (value != null) {
      // TODO: Make value be a boolean rather than String of boolean.
      return Boolean.valueOf(Bytes.toString(value)).booleanValue();
    }
    return valueIfNull;
  }

  /**
   * <em> INTERNAL </em> Used to denote if the current table represents 
   * <code> -ROOT- </code> or <code> .META. </code> region. This is used 
   * internally by the HTableDescriptor constructors 
   * 
   * @param isMeta true if its either <code> -ROOT- </code> or 
   * <code> .META. </code> region 
   */
  protected void setMetaRegion(boolean isMeta) {
    values.put(IS_META_KEY, isMeta? TRUE: FALSE);
  }

  /** 
   * Checks if the table is a <code>.META.</code> table 
   *  
   * @return true if table is <code> .META. </code> region.
   */
  public boolean isMetaTable() {
    return isMetaRegion() && !isRootRegion();
  }
 
  /**
   * Checks of the tableName being passed represents either 
   * <code > -ROOT- </code> or <code> .META. </code>
   *  
   * @return true if a tablesName is either <code> -ROOT- </code> 
   * or <code> .META. </code>
   */
  public static boolean isMetaTable(final byte [] tableName) {
    return Bytes.equals(tableName, HConstants.ROOT_TABLE_NAME) ||
      Bytes.equals(tableName, HConstants.META_TABLE_NAME);
  }

  /**
   * Check passed byte buffer, "tableName", is legal user-space table name.
   * @return Returns passed <code>tableName</code> param
   * @throws NullPointerException If passed <code>tableName</code> is null
   * @throws IllegalArgumentException if passed a tableName
   * that is made of other than 'word' characters or underscores: i.e.
   * <code>[a-zA-Z_0-9].
   */
  public static byte [] isLegalTableName(final byte [] tableName) {
    if (tableName == null || tableName.length <= 0) {
      throw new IllegalArgumentException("Name is null or empty");
    }
    if (tableName[0] == '.' || tableName[0] == '-') {
      throw new IllegalArgumentException("Illegal first character <" + tableName[0] +
          "> at 0. User-space table names can only start with 'word " +
          "characters': i.e. [a-zA-Z_0-9]: " + Bytes.toString(tableName));
    }
    for (int i = 0; i < tableName.length; i++) {
      if (Character.isLetterOrDigit(tableName[i]) || tableName[i] == '_' || 
    		  tableName[i] == '-' || tableName[i] == '.') {
        continue;
      }
      throw new IllegalArgumentException("Illegal character <" + tableName[i] +
        "> at " + i + ". User-space table names can only contain " +
        "'word characters': i.e. [a-zA-Z_0-9-.]: " + Bytes.toString(tableName));
    }
    return tableName;
  }

  /**
   * Getter for accessing the metadata associated with the key
   *  
   * @param key The key.
   * @return The value.
   * @see #values
   */
  public byte[] getValue(byte[] key) {
    return getValue(new ImmutableBytesWritable(key));
  }

  private byte[] getValue(final ImmutableBytesWritable key) {
    ImmutableBytesWritable ibw = values.get(key);
    if (ibw == null)
      return null;
    return ibw.get();
  }

  /**
   * Getter for accessing the metadata associated with the key
   *  
   * @param key The key.
   * @return The value.
   * @see #values
   */
  public String getValue(String key) {
    byte[] value = getValue(Bytes.toBytes(key));
    if (value == null)
      return null;
    return Bytes.toString(value);
  }

  /**
   * Getter for fetching an unmodifiable {@link #values} map.
   *  
   * @return unmodifiable map {@link #values}.
   * @see #values
   */
  public Map<ImmutableBytesWritable,ImmutableBytesWritable> getValues() {
     return Collections.unmodifiableMap(values);
  }

  /**
   * Setter for storing metadata as a (key, value) pair in {@link #values} map
   *  
   * @param key The key.
   * @param value The value.
   * @see #values
   */
  public void setValue(byte[] key, byte[] value) {
    setValue(new ImmutableBytesWritable(key), value);
  }

  /*
   * @param key The key.
   * @param value The value.
   */
  private void setValue(final ImmutableBytesWritable key,
      final byte[] value) {
    values.put(key, new ImmutableBytesWritable(value));
  }

  /*
   * @param key The key.
   * @param value The value.
   */
  private void setValue(final ImmutableBytesWritable key,
      final ImmutableBytesWritable value) {
    values.put(key, value);
  }

  /**
   * Setter for storing metadata as a (key, value) pair in {@link #values} map
   *  
   * @param key The key.
   * @param value The value.
   * @see #values
   */
  public void setValue(String key, String value) {
    setValue(Bytes.toBytes(key), Bytes.toBytes(value));
  }

  /**
   * Remove metadata represented by the key from the {@link #values} map
   * 
   * @param key Key whose key and value we're to remove from HTableDescriptor
   * parameters.
   */
  public void remove(final byte [] key) {
    values.remove(new ImmutableBytesWritable(key));
  }

  /**
   * Check if the readOnly flag of the table is set. If the readOnly flag is 
   * set then the contents of the table can only be read from but not modified.
   * 
   * @return true if all columns in the table should be read only
   */
  public boolean isReadOnly() {
    return isSomething(READONLY_KEY, DEFAULT_READONLY);
  }

  /**
   * Setting the table as read only sets all the columns in the table as read
   * only. By default all tables are modifiable, but if the readOnly flag is 
   * set to true then the contents of the table can only be read but not modified.
   *  
   * @param readOnly True if all of the columns in the table should be read
   * only.
   */
  public void setReadOnly(final boolean readOnly) {
    setValue(READONLY_KEY, readOnly? TRUE: FALSE);
  }

  /**
   * Check if deferred log edits are enabled on the table.  
   * 
   * @return true if that deferred log flush is enabled on the table
   * 
   * @see #setDeferredLogFlush(boolean)
   */
  public synchronized boolean isDeferredLogFlush() {
    if(this.isDeferredLog == null) {
      this.isDeferredLog =
          isSomething(DEFERRED_LOG_FLUSH_KEY, DEFAULT_DEFERRED_LOG_FLUSH);
    }
    return this.isDeferredLog;
  }

  /**
   * This is used to defer the log edits syncing to the file system. Everytime 
   * an edit is sent to the server it is first sync'd to the file system by the 
   * log writer. This sync is an expensive operation and thus can be deferred so 
   * that the edits are kept in memory for a specified period of time as represented
   * by <code> hbase.regionserver.optionallogflushinterval </code> and not flushed
   * for every edit.
   * <p>
   * NOTE:- This option might result in data loss if the region server crashes
   * before these deferred edits in memory are flushed onto the filesystem. 
   * </p>
   * 
   * @param isDeferredLogFlush
   */
  public void setDeferredLogFlush(final boolean isDeferredLogFlush) {
    setValue(DEFERRED_LOG_FLUSH_KEY, isDeferredLogFlush? TRUE: FALSE);
    this.isDeferredLog = isDeferredLogFlush;
  }

  /**
   * Get the name of the table as a byte array.
   * 
   * @return name of table 
   */
  public byte [] getName() {
    return name;
  }

  /**
   * Get the name of the table as a String
   * 
   * @return name of table as a String 
   */
  public String getNameAsString() {
    return this.nameAsString;
  }
  
  /**
   * This get the class associated with the region split policy which 
   * determines when a region split should occur.  The class used by
   * default is {@link org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy}
   * which split the region base on a constant {@link #getMaxFileSize()}
   * 
   * @return the class name of the region split policy for this table.
   * If this returns null, the default constant size based split policy
   * is used.
   */
   public String getRegionSplitPolicyClassName() {
    return getValue(SPLIT_POLICY);
  }

  /**
   * Set the name of the table. 
   * 
   * @param name name of table 
   */
  public void setName(byte[] name) {
    this.name = name;
    this.nameAsString = Bytes.toString(this.name);
    setMetaFlags(this.name);
  }

  /** 
   * Returns the maximum size upto which a region can grow to after which a region
   * split is triggered. The region size is represented by the size of the biggest 
   * store file in that region.
   * 
   * @return max hregion size for table
   * 
   * @see #setMaxFileSize(long)
   */
  public long getMaxFileSize() {
    byte [] value = getValue(MAX_FILESIZE_KEY);
    if (value != null)
      return Long.valueOf(Bytes.toString(value)).longValue();
    return HConstants.DEFAULT_MAX_FILE_SIZE;
  }
  
  /**
   * Sets the maximum size upto which a region can grow to after which a region
   * split is triggered. The region size is represented by the size of the biggest 
   * store file in that region, i.e. If the biggest store file grows beyond the 
   * maxFileSize, then the region split is triggered. This defaults to a value of 
   * 256 MB.
   * <p>
   * This is not an absolute value and might vary. Assume that a single row exceeds 
   * the maxFileSize then the storeFileSize will be greater than maxFileSize since
   * a single row cannot be split across multiple regions 
   * </p>
   * 
   * @param maxFileSize The maximum file size that a store file can grow to
   * before a split is triggered.
   */
  public void setMaxFileSize(long maxFileSize) {
    setValue(MAX_FILESIZE_KEY, Bytes.toBytes(Long.toString(maxFileSize)));
  }

  /**
   * Returns the size of the memstore after which a flush to filesystem is triggered.
   * 
   * @return memory cache flush size for each hregion
   * 
   * @see #setMemStoreFlushSize(long)
   */
  public long getMemStoreFlushSize() {
    byte [] value = getValue(MEMSTORE_FLUSHSIZE_KEY);
    if (value != null)
      return Long.valueOf(Bytes.toString(value)).longValue();
    return DEFAULT_MEMSTORE_FLUSH_SIZE;
  }

  /**
   * Represents the maximum size of the memstore after which the contents of the 
   * memstore are flushed to the filesystem. This defaults to a size of 64 MB.
   * 
   * @param memstoreFlushSize memory cache flush size for each hregion
   */
  public void setMemStoreFlushSize(long memstoreFlushSize) {
    setValue(MEMSTORE_FLUSHSIZE_KEY,
      Bytes.toBytes(Long.toString(memstoreFlushSize)));
  }

  /**
   * Adds a column family.
   * @param family HColumnDescriptor of family to add.
   */
  public void addFamily(final HColumnDescriptor family) {
    if (family.getName() == null || family.getName().length <= 0) {
      throw new NullPointerException("Family name cannot be null or empty");
    }
    this.families.put(family.getName(), family);
  }

  /**
   * Checks to see if this table contains the given column family
   * @param familyName Family name or column name.
   * @return true if the table contains the specified family name
   */
  public boolean hasFamily(final byte [] familyName) {
    return families.containsKey(familyName);
  }

  /**
   * @return Name of this table and then a map of all of the column family
   * descriptors.
   * @see #getNameAsString()
   */
  @Override
  public String toString() {
    StringBuilder s = new StringBuilder();
    s.append('{');
    s.append(HConstants.NAME);
    s.append(" => '");
    s.append(Bytes.toString(name));
    s.append("'");
    for (Map.Entry<ImmutableBytesWritable, ImmutableBytesWritable> e:
        values.entrySet()) {
      String key = Bytes.toString(e.getKey().get());
      String value = Bytes.toString(e.getValue().get());
      if (key == null) {
        continue;
      }
      String upperCase = key.toUpperCase();
      if (upperCase.equals(IS_ROOT) || upperCase.equals(IS_META)) {
        // Skip. Don't bother printing out read-only values if false.
        if (value.toLowerCase().equals(Boolean.FALSE.toString())) {
          continue;
        }
      }
      s.append(", ");
      s.append(Bytes.toString(e.getKey().get()));
      s.append(" => '");
      s.append(Bytes.toString(e.getValue().get()));
      s.append("'");
    }
    s.append(", ");
    s.append(FAMILIES);
    s.append(" => ");
    s.append(families.values());
    s.append('}');
    return s.toString();
  }

  /**
   * @return Name of this table and then a map of all of the column family
   * descriptors (with only the non-default column family attributes)
   */
  public String toStringCustomizedValues() {
    StringBuilder s = new StringBuilder();
    s.append('{');
    s.append(HConstants.NAME);
    s.append(" => '");
    s.append(Bytes.toString(name));
    s.append("'");
    for (Map.Entry<ImmutableBytesWritable, ImmutableBytesWritable> e:
        values.entrySet()) {
      String key = Bytes.toString(e.getKey().get());
      String value = Bytes.toString(e.getValue().get());
      if (key == null) {
        continue;
      }
      String upperCase = key.toUpperCase();
      if (upperCase.equals(IS_ROOT) || upperCase.equals(IS_META)) {
        // Skip. Don't bother printing out read-only values if false.
        if (value.toLowerCase().equals(Boolean.FALSE.toString())) {
          continue;
        }
      }
      s.append(", ");
      s.append(Bytes.toString(e.getKey().get()));
      s.append(" => '");
      s.append(Bytes.toString(e.getValue().get()));
      s.append("'");
    }
    s.append(", ");
    s.append(FAMILIES);
    s.append(" => [");
    int size = families.values().size();
    int i = 0;
    for(HColumnDescriptor hcd : families.values()) {
      s.append(hcd.toStringCustomizedValues());
      i++;
      if( i != size)
        s.append(", ");
    }
    s.append("]}");
    return s.toString();
  }

  /**
   * Compare the contents of the descriptor with another one passed as a parameter. 
   * Checks if the obj passed is an instance of HTableDescriptor, if yes then the
   * contents of the descriptors are compared.
   * 
   * @return true if the contents of the the two descriptors exactly match
   * 
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof HTableDescriptor)) {
      return false;
    }
    return compareTo((HTableDescriptor)obj) == 0;
  }

  /**
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    int result = Bytes.hashCode(this.name);
    result ^= Byte.valueOf(TABLE_DESCRIPTOR_VERSION).hashCode();
    if (this.families != null && this.families.size() > 0) {
      for (HColumnDescriptor e: this.families.values()) {
        result ^= e.hashCode();
      }
    }
    result ^= values.hashCode();
    return result;
  }

  // Writable
  /**
   * <em> INTERNAL </em> This method is a part of {@link WritableComparable} interface 
   * and is used for de-serialization of the HTableDescriptor over RPC
   */
  public void readFields(DataInput in) throws IOException {
    int version = in.readInt();
    if (version < 3)
      throw new IOException("versions < 3 are not supported (and never existed!?)");
    // version 3+
    name = Bytes.readByteArray(in);
    nameAsString = Bytes.toString(this.name);
    setRootRegion(in.readBoolean());
    setMetaRegion(in.readBoolean());
    values.clear();
    int numVals = in.readInt();
    for (int i = 0; i < numVals; i++) {
      ImmutableBytesWritable key = new ImmutableBytesWritable();
      ImmutableBytesWritable value = new ImmutableBytesWritable();
      key.readFields(in);
      value.readFields(in);
      values.put(key, value);
    }
    families.clear();
    int numFamilies = in.readInt();
    for (int i = 0; i < numFamilies; i++) {
      HColumnDescriptor c = new HColumnDescriptor();
      c.readFields(in);
      families.put(c.getName(), c);
    }
    if (version < 4) {
      return;
    }
  }

  /**
   * <em> INTERNAL </em> This method is a part of {@link WritableComparable} interface 
   * and is used for serialization of the HTableDescriptor over RPC
   */
  public void write(DataOutput out) throws IOException {
	out.writeInt(TABLE_DESCRIPTOR_VERSION);
    Bytes.writeByteArray(out, name);
    out.writeBoolean(isRootRegion());
    out.writeBoolean(isMetaRegion());
    out.writeInt(values.size());
    for (Map.Entry<ImmutableBytesWritable, ImmutableBytesWritable> e:
        values.entrySet()) {
      e.getKey().write(out);
      e.getValue().write(out);
    }
    out.writeInt(families.size());
    for(Iterator<HColumnDescriptor> it = families.values().iterator();
        it.hasNext(); ) {
      HColumnDescriptor family = it.next();
      family.write(out);
    }
  }

  // Comparable

  /**
   * Compares the descriptor with another descriptor which is passed as a parameter.
   * This compares the content of the two descriptors and not the reference.
   * 
   * @return 0 if the contents of the descriptors are exactly matching, 
   * 		 1 if there is a mismatch in the contents 
   */
  public int compareTo(final HTableDescriptor other) {
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
    if (result == 0) {
      // punt on comparison for ordering, just calculate difference
      result = this.values.hashCode() - other.values.hashCode();
      if (result < 0)
        result = -1;
      else if (result > 0)
        result = 1;
    }
    return result;
  }

  /**
   * Returns an unmodifiable collection of all the {@link HColumnDescriptor} 
   * of all the column families of the table.
   *  
   * @return Immutable collection of {@link HColumnDescriptor} of all the
   * column families. 
   */
  public Collection<HColumnDescriptor> getFamilies() {
    return Collections.unmodifiableCollection(this.families.values());
  }

  /**
   * Returns all the column family names of the current table. The map of 
   * HTableDescriptor contains mapping of family name to HColumnDescriptors. 
   * This returns all the keys of the family map which represents the column 
   * family names of the table. 
   * 
   * @return Immutable sorted set of the keys of the families.
   */
  public Set<byte[]> getFamiliesKeys() {
    return Collections.unmodifiableSet(this.families.keySet());
  }

  /** 
   * Returns an array all the {@link HColumnDescriptor} of the column families 
   * of the table.
   *  
   * @return Array of all the HColumnDescriptors of the current table 
   * 
   * @see #getFamilies()
   */
  public HColumnDescriptor[] getColumnFamilies() {
    return getFamilies().toArray(new HColumnDescriptor[0]);
  }
  

  /**
   * Returns the HColumnDescriptor for a specific column family with name as 
   * specified by the parameter column.
   * 
   * @param column Column family name 
   * @return Column descriptor for the passed family name or the family on
   * passed in column.
   */
  public HColumnDescriptor getFamily(final byte [] column) {
    return this.families.get(column);
  }
  

  /**
   * Removes the HColumnDescriptor with name specified by the parameter column 
   * from the table descriptor
   * 
   * @param column Name of the column family to be removed.
   * @return Column descriptor for the passed family name or the family on
   * passed in column.
   */
  public HColumnDescriptor removeFamily(final byte [] column) {
    return this.families.remove(column);
  }
  

  /**
   * Add a table coprocessor to this table. The coprocessor
   * type must be {@link org.apache.hadoop.hbase.coprocessor.RegionObserver}
   * or Endpoint.
   * It won't check if the class can be loaded or not.
   * Whether a coprocessor is loadable or not will be determined when
   * a region is opened.
   * @param className Full class name.
   * @throws IOException
   */
  public void addCoprocessor(String className) throws IOException {
    addCoprocessor(className, null, Coprocessor.PRIORITY_USER, null);
  }

  
  /**
   * Add a table coprocessor to this table. The coprocessor
   * type must be {@link org.apache.hadoop.hbase.coprocessor.RegionObserver}
   * or Endpoint.
   * It won't check if the class can be loaded or not.
   * Whether a coprocessor is loadable or not will be determined when
   * a region is opened.
   * @param jarFilePath Path of the jar file. If it's null, the class will be
   * loaded from default classloader.
   * @param className Full class name.
   * @param priority Priority
   * @param kvs Arbitrary key-value parameter pairs passed into the coprocessor.
   * @throws IOException
   */
  public void addCoprocessor(String className, Path jarFilePath,
                             int priority, final Map<String, String> kvs)
  throws IOException {
    if (hasCoprocessor(className)) {
      throw new IOException("Coprocessor " + className + " already exists.");
    }
    // validate parameter kvs
    StringBuilder kvString = new StringBuilder();
    if (kvs != null) {
      for (Map.Entry<String, String> e: kvs.entrySet()) {
        if (!e.getKey().matches(HConstants.CP_HTD_ATTR_VALUE_PARAM_KEY_PATTERN)) {
          throw new IOException("Illegal parameter key = " + e.getKey());
        }
        if (!e.getValue().matches(HConstants.CP_HTD_ATTR_VALUE_PARAM_VALUE_PATTERN)) {
          throw new IOException("Illegal parameter (" + e.getKey() +
              ") value = " + e.getValue());
        }
        if (kvString.length() != 0) {
          kvString.append(',');
        }
        kvString.append(e.getKey());
        kvString.append('=');
        kvString.append(e.getValue());
      }
    }

    // generate a coprocessor key
    int maxCoprocessorNumber = 0;
    Matcher keyMatcher;
    for (Map.Entry<ImmutableBytesWritable, ImmutableBytesWritable> e:
        this.values.entrySet()) {
      keyMatcher =
          HConstants.CP_HTD_ATTR_KEY_PATTERN.matcher(
              Bytes.toString(e.getKey().get()));
      if (!keyMatcher.matches()) {
        continue;
      }
      maxCoprocessorNumber = Math.max(Integer.parseInt(keyMatcher.group(1)),
          maxCoprocessorNumber);
    }
    maxCoprocessorNumber++;

    String key = "coprocessor$" + Integer.toString(maxCoprocessorNumber);
    String value = ((jarFilePath == null)? "" : jarFilePath.toString()) +
        "|" + className + "|" + Integer.toString(priority) + "|" +
        kvString.toString();
    setValue(key, value);
  }

  
  /**
   * Check if the table has an attached co-processor represented by the name className
   * 
   * @param className - Class name of the co-processor
   * @return true of the table has a co-processor className
   */
  public boolean hasCoprocessor(String className) {
    Matcher keyMatcher;
    Matcher valueMatcher;
    for (Map.Entry<ImmutableBytesWritable, ImmutableBytesWritable> e:
        this.values.entrySet()) {
      keyMatcher =
          HConstants.CP_HTD_ATTR_KEY_PATTERN.matcher(
              Bytes.toString(e.getKey().get()));
      if (!keyMatcher.matches()) {
        continue;
      }
      valueMatcher =
        HConstants.CP_HTD_ATTR_VALUE_PATTERN.matcher(
            Bytes.toString(e.getValue().get()));
      if (!valueMatcher.matches()) {
        continue;
      }
      // get className and compare
      String clazz = valueMatcher.group(2).trim(); // classname is the 2nd field
      if (clazz.equals(className.trim())) {
        return true;
      }
    }
    return false;
  }

  
  /**
   * Returns the {@link Path} object representing the table directory under 
   * path rootdir 
   * 
   * @param rootdir qualified path of HBase root directory
   * @param tableName name of table
   * @return {@link Path} for table
   */
  public static Path getTableDir(Path rootdir, final byte [] tableName) {
    return new Path(rootdir, Bytes.toString(tableName));
  }

  /** Table descriptor for <core>-ROOT-</code> catalog table */
  public static final HTableDescriptor ROOT_TABLEDESC = new HTableDescriptor(
      HConstants.ROOT_TABLE_NAME,
      new HColumnDescriptor[] { new HColumnDescriptor(HConstants.CATALOG_FAMILY,
          10,  // Ten is arbitrary number.  Keep versions to help debugging.
          Compression.Algorithm.NONE.getName(), true, true, 8 * 1024,
          HConstants.FOREVER, StoreFile.BloomType.NONE.toString(),  
          HConstants.REPLICATION_SCOPE_LOCAL) });

  /** Table descriptor for <code>.META.</code> catalog table */
  public static final HTableDescriptor META_TABLEDESC = new HTableDescriptor(
      HConstants.META_TABLE_NAME, new HColumnDescriptor[] {
          new HColumnDescriptor(HConstants.CATALOG_FAMILY,
            10, // Ten is arbitrary number.  Keep versions to help debugging.
            Compression.Algorithm.NONE.getName(), true, true, 8 * 1024,
            HConstants.FOREVER, StoreFile.BloomType.NONE.toString(),
            HConstants.REPLICATION_SCOPE_LOCAL)});


  public void setOwner(User owner) {
    setOwnerString(owner != null ? owner.getShortName() : null);
  }

  // used by admin.rb:alter(table_name,*args) to update owner.
  public void setOwnerString(String ownerString) {
    if (ownerString != null) {
      setValue(OWNER_KEY, Bytes.toBytes(ownerString));
    } else {
      values.remove(OWNER_KEY);
    }
  }

  public String getOwnerString() {
    if (getValue(OWNER_KEY) != null) {
      return Bytes.toString(getValue(OWNER_KEY));
    }
    // Note that every table should have an owner (i.e. should have OWNER_KEY set).
    // .META. and -ROOT- should return system user as owner, not null (see MasterFileSystem.java:bootstrap()).
    return null;
  }
}
