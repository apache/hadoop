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
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableComparable;

/**
 * HTableDescriptor contains the name of an HTable, and its
 * column families.
 */
public class HTableDescriptor implements WritableComparable<HTableDescriptor> {

  // Changes prior to version 3 were not recorded here.
  // Version 3 adds metadata as a map where keys and values are byte[].
  // Version 4 adds indexes
  // Version 5 removed transactional pollution -- e.g. indexes
  public static final byte TABLE_DESCRIPTOR_VERSION = 5;

  private byte [] name = HConstants.EMPTY_BYTE_ARRAY;
  private String nameAsString = "";

  // Table metadata
  protected Map<ImmutableBytesWritable, ImmutableBytesWritable> values =
    new HashMap<ImmutableBytesWritable, ImmutableBytesWritable>();

  public static final String FAMILIES = "FAMILIES";
  public static final ImmutableBytesWritable FAMILIES_KEY =
    new ImmutableBytesWritable(Bytes.toBytes(FAMILIES));
  public static final String MAX_FILESIZE = "MAX_FILESIZE";
  public static final ImmutableBytesWritable MAX_FILESIZE_KEY =
    new ImmutableBytesWritable(Bytes.toBytes(MAX_FILESIZE));
  public static final String READONLY = "READONLY";
  public static final ImmutableBytesWritable READONLY_KEY =
    new ImmutableBytesWritable(Bytes.toBytes(READONLY));
  public static final String MEMSTORE_FLUSHSIZE = "MEMSTORE_FLUSHSIZE";
  public static final ImmutableBytesWritable MEMSTORE_FLUSHSIZE_KEY =
    new ImmutableBytesWritable(Bytes.toBytes(MEMSTORE_FLUSHSIZE));
  public static final String IS_ROOT = "IS_ROOT";
  public static final ImmutableBytesWritable IS_ROOT_KEY =
    new ImmutableBytesWritable(Bytes.toBytes(IS_ROOT));
  public static final String IS_META = "IS_META";

  public static final ImmutableBytesWritable IS_META_KEY =
    new ImmutableBytesWritable(Bytes.toBytes(IS_META));

  public static final String DEFERRED_LOG_FLUSH = "DEFERRED_LOG_FLUSH";
  public static final ImmutableBytesWritable DEFERRED_LOG_FLUSH_KEY =
    new ImmutableBytesWritable(Bytes.toBytes(DEFERRED_LOG_FLUSH));


  // The below are ugly but better than creating them each time till we
  // replace booleans being saved as Strings with plain booleans.  Need a
  // migration script to do this.  TODO.
  private static final ImmutableBytesWritable FALSE =
    new ImmutableBytesWritable(Bytes.toBytes(Boolean.FALSE.toString()));
  private static final ImmutableBytesWritable TRUE =
    new ImmutableBytesWritable(Bytes.toBytes(Boolean.TRUE.toString()));

  public static final boolean DEFAULT_READONLY = false;

  public static final long DEFAULT_MEMSTORE_FLUSH_SIZE = 1024*1024*64L;

  public static final long DEFAULT_MAX_FILESIZE = 1024*1024*256L;

  public static final boolean DEFAULT_DEFERRED_LOG_FLUSH = false;

  private volatile Boolean meta = null;
  private volatile Boolean root = null;
  private Boolean isDeferredLog = null;

  // Key is hash of the family name.
  public final Map<byte [], HColumnDescriptor> families =
    new TreeMap<byte [], HColumnDescriptor>(Bytes.BYTES_RAWCOMPARATOR);

  /**
   * Private constructor used internally creating table descriptors for
   * catalog tables: e.g. .META. and -ROOT-.
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
   * Private constructor used internally creating table descriptors for
   * catalog tables: e.g. .META. and -ROOT-.
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
   * Constructor.
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
   * Called by constructors.
   * @param name
   */
  private void setMetaFlags(final byte [] name) {
    setRootRegion(Bytes.equals(name, HConstants.ROOT_TABLE_NAME));
    setMetaRegion(isRootRegion() ||
      Bytes.equals(name, HConstants.META_TABLE_NAME));
  }

  /** @return true if this is the root region */
  public boolean isRootRegion() {
    if (this.root == null) {
      this.root = isSomething(IS_ROOT_KEY, false)? Boolean.TRUE: Boolean.FALSE;
    }
    return this.root.booleanValue();
  }

  /** @param isRoot true if this is the root region */
  protected void setRootRegion(boolean isRoot) {
    // TODO: Make the value a boolean rather than String of boolean.
    values.put(IS_ROOT_KEY, isRoot? TRUE: FALSE);
  }

  /** @return true if this is a meta region (part of the root or meta tables) */
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
   * @param isMeta true if this is a meta region (part of the root or meta
   * tables) */
  protected void setMetaRegion(boolean isMeta) {
    values.put(IS_META_KEY, isMeta? TRUE: FALSE);
  }

  /** @return true if table is a meta table, either <code>.META.</code> or
   * <code>-ROOT-</code>
   */
  public boolean isMetaTable() {
    return isMetaRegion() && !isRootRegion();
  }

  /**
   * @param n Table name.
   * @return True if a catalog table, -ROOT- or .META.
   */
  public static boolean isMetaTable(final byte [] n) {
    return Bytes.equals(n, HConstants.ROOT_TABLE_NAME) ||
      Bytes.equals(n, HConstants.META_TABLE_NAME);
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
    if (b[0] == '.' || b[0] == '-') {
      throw new IllegalArgumentException("Illegal first character <" + b[0] +
          "> at 0. User-space table names can only start with 'word " +
          "characters': i.e. [a-zA-Z_0-9]: " + Bytes.toString(b));
    }
    for (int i = 0; i < b.length; i++) {
      if (Character.isLetterOrDigit(b[i]) || b[i] == '_' || b[i] == '-' ||
          b[i] == '.') {
        continue;
      }
      throw new IllegalArgumentException("Illegal character <" + b[i] +
        "> at " + i + ". User-space table names can only contain " +
        "'word characters': i.e. [a-zA-Z_0-9-.]: " + Bytes.toString(b));
    }
    return b;
  }

  /**
   * @param key The key.
   * @return The value.
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
   * @param key The key.
   * @return The value as a string.
   */
  public String getValue(String key) {
    byte[] value = getValue(Bytes.toBytes(key));
    if (value == null)
      return null;
    return Bytes.toString(value);
  }

  /**
   * @return All values.
   */
  public Map<ImmutableBytesWritable,ImmutableBytesWritable> getValues() {
     return Collections.unmodifiableMap(values);
  }

  /**
   * @param key The key.
   * @param value The value.
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
   * @param key The key.
   * @param value The value.
   */
  public void setValue(String key, String value) {
    setValue(Bytes.toBytes(key), Bytes.toBytes(value));
  }

  /**
   * @param key Key whose key and value we're to remove from HTD parameters.
   */
  public void remove(final byte [] key) {
    values.remove(new ImmutableBytesWritable(key));
  }

  /**
   * @return true if all columns in the table should be read only
   */
  public boolean isReadOnly() {
    return isSomething(READONLY_KEY, DEFAULT_READONLY);
  }

  /**
   * @param readOnly True if all of the columns in the table should be read
   * only.
   */
  public void setReadOnly(final boolean readOnly) {
    setValue(READONLY_KEY, readOnly? TRUE: FALSE);
  }

  /**
   * @return true if that table's log is hflush by other means
   */
  public synchronized boolean isDeferredLogFlush() {
    if(this.isDeferredLog == null) {
      this.isDeferredLog =
          isSomething(DEFERRED_LOG_FLUSH_KEY, DEFAULT_DEFERRED_LOG_FLUSH);
    }
    return this.isDeferredLog;
  }

  /**
   * @param isDeferredLogFlush true if that table's log is hlfush by oter means
   * only.
   */
  public void setDeferredLogFlush(final boolean isDeferredLogFlush) {
    setValue(DEFERRED_LOG_FLUSH_KEY, isDeferredLogFlush? TRUE: FALSE);
    this.isDeferredLog = isDeferredLogFlush;
  }

  /** @return name of table */
  public byte [] getName() {
    return name;
  }

  /** @return name of table */
  public String getNameAsString() {
    return this.nameAsString;
  }

  /** @return max hregion size for table */
  public long getMaxFileSize() {
    byte [] value = getValue(MAX_FILESIZE_KEY);
    if (value != null)
      return Long.valueOf(Bytes.toString(value)).longValue();
    return HConstants.DEFAULT_MAX_FILE_SIZE;
  }

  /** @param name name of table */
  public void setName(byte[] name) {
    this.name = name;
    this.nameAsString = Bytes.toString(this.name);
    setMetaFlags(this.name);
  }

  /**
   * @param maxFileSize The maximum file size that a store file can grow to
   * before a split is triggered.
   */
  public void setMaxFileSize(long maxFileSize) {
    setValue(MAX_FILESIZE_KEY, Bytes.toBytes(Long.toString(maxFileSize)));
  }

  /**
   * @return memory cache flush size for each hregion
   */
  public long getMemStoreFlushSize() {
    byte [] value = getValue(MEMSTORE_FLUSHSIZE_KEY);
    if (value != null)
      return Long.valueOf(Bytes.toString(value)).longValue();
    return DEFAULT_MEMSTORE_FLUSH_SIZE;
  }

  /**
   * @param memstoreFlushSize memory cache flush size for each hregion
   */
  public void setMemStoreFlushSize(long memstoreFlushSize) {
    setValue(MEMSTORE_FLUSHSIZE_KEY,
      Bytes.toBytes(Long.toString(memstoreFlushSize)));
  }

  /**
   * Adds a column family.
   * @param family HColumnDescriptor of familyto add.
   */
  public void addFamily(final HColumnDescriptor family) {
    if (family.getName() == null || family.getName().length <= 0) {
      throw new NullPointerException("Family name cannot be null or empty");
    }
    this.families.put(family.getName(), family);
  }

  /**
   * Checks to see if this table contains the given column family
   * @param c Family name or column name.
   * @return true if the table contains the specified family name
   */
  public boolean hasFamily(final byte [] c) {
    return families.containsKey(c);
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
   * @return Immutable sorted map of families.
   */
  public Collection<HColumnDescriptor> getFamilies() {
    return Collections.unmodifiableCollection(this.families.values());
  }

  /**
   * @return Immutable sorted set of the keys of the families.
   */
  public Set<byte[]> getFamiliesKeys() {
    return Collections.unmodifiableSet(this.families.keySet());
  }

  public HColumnDescriptor[] getColumnFamilies() {
    return getFamilies().toArray(new HColumnDescriptor[0]);
  }

  /**
   * @param column
   * @return Column descriptor for the passed family name or the family on
   * passed in column.
   */
  public HColumnDescriptor getFamily(final byte [] column) {
    return this.families.get(column);
  }

  /**
   * @param column
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
   * @param rootdir qualified path of HBase root directory
   * @param tableName name of table
   * @return path for table
   */
  public static Path getTableDir(Path rootdir, final byte [] tableName) {
    return new Path(rootdir, Bytes.toString(tableName));
  }

  /** Table descriptor for <core>-ROOT-</code> catalog table */
  public static final HTableDescriptor ROOT_TABLEDESC = new HTableDescriptor(
      HConstants.ROOT_TABLE_NAME,
      new HColumnDescriptor[] { new HColumnDescriptor(HConstants.CATALOG_FAMILY,
          10,  // Ten is arbitrary number.  Keep versions to help debuggging.
          Compression.Algorithm.NONE.getName(), true, true, 8 * 1024,
          HConstants.FOREVER, StoreFile.BloomType.NONE.toString(),  
          HConstants.REPLICATION_SCOPE_LOCAL) });

  /** Table descriptor for <code>.META.</code> catalog table */
  public static final HTableDescriptor META_TABLEDESC = new HTableDescriptor(
      HConstants.META_TABLE_NAME, new HColumnDescriptor[] {
          new HColumnDescriptor(HConstants.CATALOG_FAMILY,
            10, // Ten is arbitrary number.  Keep versions to help debuggging.
            Compression.Algorithm.NONE.getName(), true, true, 8 * 1024,
            HConstants.FOREVER, StoreFile.BloomType.NONE.toString(),
            HConstants.REPLICATION_SCOPE_LOCAL)});
}
