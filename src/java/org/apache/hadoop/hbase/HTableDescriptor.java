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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.tableindexed.IndexSpecification;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.rest.exception.HBaseRestException;
import org.apache.hadoop.hbase.rest.serializer.IRestSerializer;
import org.apache.hadoop.hbase.rest.serializer.ISerializable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableComparable;

import agilejson.TOJSON;

/**
 * HTableDescriptor contains the name of an HTable, and its
 * column families.
 */
public class HTableDescriptor implements WritableComparable<HTableDescriptor>, ISerializable {

  // Changes prior to version 3 were not recorded here.
  // Version 3 adds metadata as a map where keys and values are byte[].
  // Version 4 adds indexes
  public static final byte TABLE_DESCRIPTOR_VERSION = 4;

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
  public static final String MEMCACHE_FLUSHSIZE = "MEMCACHE_FLUSHSIZE";
  public static final ImmutableBytesWritable MEMCACHE_FLUSHSIZE_KEY =
    new ImmutableBytesWritable(Bytes.toBytes(MEMCACHE_FLUSHSIZE));
  public static final String IS_ROOT = "IS_ROOT";
  public static final ImmutableBytesWritable IS_ROOT_KEY =
    new ImmutableBytesWritable(Bytes.toBytes(IS_ROOT));
  public static final String IS_META = "IS_META";

  public static final ImmutableBytesWritable IS_META_KEY =
    new ImmutableBytesWritable(Bytes.toBytes(IS_META));


  // The below are ugly but better than creating them each time till we
  // replace booleans being saved as Strings with plain booleans.  Need a
  // migration script to do this.  TODO.
  private static final ImmutableBytesWritable FALSE =
    new ImmutableBytesWritable(Bytes.toBytes(Boolean.FALSE.toString()));
  private static final ImmutableBytesWritable TRUE =
    new ImmutableBytesWritable(Bytes.toBytes(Boolean.TRUE.toString()));

  public static final boolean DEFAULT_IN_MEMORY = false;

  public static final boolean DEFAULT_READONLY = false;

  public static final int DEFAULT_MEMCACHE_FLUSH_SIZE = 1024*1024*64;
  
  public static final int DEFAULT_MAX_FILESIZE = 1024*1024*256;
    
  private volatile Boolean meta = null;
  private volatile Boolean root = null;

  // Key is hash of the family name.
  private final Map<Integer, HColumnDescriptor> families =
    new HashMap<Integer, HColumnDescriptor>();

  // Key is indexId
  private final Map<String, IndexSpecification> indexes =
    new HashMap<String, IndexSpecification>();
  
  /**
   * Private constructor used internally creating table descriptors for 
   * catalog tables: e.g. .META. and -ROOT-.
   */
  protected HTableDescriptor(final byte [] name, HColumnDescriptor[] families) {
    this.name = name.clone();
    this.nameAsString = Bytes.toString(this.name);
    setMetaFlags(name);
    for(HColumnDescriptor descriptor : families) {
      this.families.put(Bytes.mapKey(descriptor.getName()), descriptor);
    }
  }

  /**
   * Private constructor used internally creating table descriptors for 
   * catalog tables: e.g. .META. and -ROOT-.
   */
  protected HTableDescriptor(final byte [] name, HColumnDescriptor[] families,
      Collection<IndexSpecification> indexes,
       Map<ImmutableBytesWritable,ImmutableBytesWritable> values) {
    this.name = name.clone();
    this.nameAsString = Bytes.toString(this.name);
    setMetaFlags(name);
    for(HColumnDescriptor descriptor : families) {
      this.families.put(Bytes.mapKey(descriptor.getName()), descriptor);
    }
    for(IndexSpecification index : indexes) {
      this.indexes.put(index.getIndexId(), index);
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
      this.families.put(Bytes.mapKey(c.getName()), new HColumnDescriptor(c));
    }
    for (Map.Entry<ImmutableBytesWritable, ImmutableBytesWritable> e:
        desc.values.entrySet()) {
      this.values.put(e.getKey(), e.getValue());
    }
    this.indexes.putAll(desc.indexes);
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

  /** @return true if table is the meta table */
  public boolean isMetaTable() {
    return isMetaRegion() && !isRootRegion();
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
          ">. " + "User-space table names can only start with 'word " +
          "characters': i.e. [a-zA-Z_0-9]: " + Bytes.toString(b));
    }
    for (int i = 0; i < b.length; i++) {
      if (Character.isLetterOrDigit(b[i]) || b[i] == '_' || b[i] == '-' ||
          b[i] == '.') {
        continue;
      }
      throw new IllegalArgumentException("Illegal character <" + b[i] + ">. " +
        "User-space table names can only contain 'word characters':" +
        "i.e. [a-zA-Z_0-9-.]: " + Bytes.toString(b));
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
   * @return true if all columns in the table should be kept in the 
   * HRegionServer cache only
   */
  public boolean isInMemory() {
    String value = getValue(HConstants.IN_MEMORY);
    if (value != null)
      return Boolean.valueOf(value).booleanValue();
    return DEFAULT_IN_MEMORY;
  }

  /**
   * @param inMemory True if all of the columns in the table should be kept in
   * the HRegionServer cache only.
   */
  public void setInMemory(boolean inMemory) {
    setValue(HConstants.IN_MEMORY, Boolean.toString(inMemory));
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

  /** @return name of table */
  @TOJSON
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
  public int getMemcacheFlushSize() {
    byte [] value = getValue(MEMCACHE_FLUSHSIZE_KEY);
    if (value != null)
      return Integer.valueOf(Bytes.toString(value)).intValue();
    return DEFAULT_MEMCACHE_FLUSH_SIZE;
  }
  
  /**
   * @param memcacheFlushSize memory cache flush size for each hregion
   */
  public void setMemcacheFlushSize(int memcacheFlushSize) {
    setValue(MEMCACHE_FLUSHSIZE_KEY,
      Bytes.toBytes(Integer.toString(memcacheFlushSize)));
  }
    
  public Collection<IndexSpecification> getIndexes() {
    return indexes.values();
  }
  
  public IndexSpecification getIndex(String indexId) {
    return indexes.get(indexId);
  }
  
  public void addIndex(IndexSpecification index) {
    indexes.put(index.getIndexId(), index);
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
    StringBuffer s = new StringBuffer();
    s.append('{');
    s.append(HConstants.NAME);
    s.append(" => '");
    s.append(Bytes.toString(name));
    s.append("'");
    for (Map.Entry<ImmutableBytesWritable, ImmutableBytesWritable> e:
        values.entrySet()) {
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

    s.append(", ");
    s.append("INDEXES");
    s.append(" => ");
    s.append(indexes.values());
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
      families.put(Bytes.mapKey(c.getName()), c);
    }
    indexes.clear();
    if (version < 4) {
      return;
    }
    int numIndexes = in.readInt();
    for (int i = 0; i < numIndexes; i++) {
      IndexSpecification index = new IndexSpecification();
      index.readFields(in);
      addIndex(index);
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
    out.writeInt(indexes.size());
    for(IndexSpecification index : indexes.values()) {
      index.write(out);
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
  
  @TOJSON(fieldName = "columns")
  public HColumnDescriptor[] getColumnFamilies() {
    return getFamilies().toArray(new HColumnDescriptor[0]);
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

  /** Table descriptor for <core>-ROOT-</code> catalog table */
  public static final HTableDescriptor ROOT_TABLEDESC = new HTableDescriptor(
      HConstants.ROOT_TABLE_NAME,
      new HColumnDescriptor[] { new HColumnDescriptor(HConstants.COLUMN_FAMILY,
          10,  // Ten is arbitrary number.  Keep versions to help debuggging.
          Compression.Algorithm.NONE.getName(), false, true, 8 * 1024,
          Integer.MAX_VALUE, HConstants.FOREVER, false) });
  
  /** Table descriptor for <code>.META.</code> catalog table */
  public static final HTableDescriptor META_TABLEDESC = new HTableDescriptor(
      HConstants.META_TABLE_NAME, new HColumnDescriptor[] {
          new HColumnDescriptor(HConstants.COLUMN_FAMILY,
            10, // Ten is arbitrary number.  Keep versions to help debuggging.
            Compression.Algorithm.NONE.getName(), false, true, 8 * 1024,
            Integer.MAX_VALUE, HConstants.FOREVER, false),
          new HColumnDescriptor(HConstants.COLUMN_FAMILY_HISTORIAN,
            HConstants.ALL_VERSIONS, Compression.Algorithm.NONE.getName(),
            false, false,  8 * 1024,
            Integer.MAX_VALUE, HConstants.WEEK_IN_SECONDS, false)});

  /* (non-Javadoc)
   * @see org.apache.hadoop.hbase.rest.xml.IOutputXML#toXML()
   */
  public void restSerialize(IRestSerializer serializer) throws HBaseRestException {
    serializer.serializeTableDescriptor(this);
  }
}