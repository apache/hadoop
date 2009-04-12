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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.RawComparator;

/**
 * An HBase Key/Value.  Instances of this class are immutable.  They are not
 * comparable but Comparators are provided  Comparators change with context,
 * whether user table or a catalog table comparison context.  Its
 * important that you use the appropriate comparator comparing rows in
 * particular.  There are Comparators for KeyValue instances and then for
 * just the Key portion of a KeyValue used mostly in {@link HFile}.
 * 
 * <p>KeyValue wraps a byte array and has offset and length for passed array
 * at where to start interpreting the content as a KeyValue blob.  The KeyValue
 * blob format inside the byte array is:
 * <code>&lt;keylength> &lt;valuelength> &lt;key> &lt;value></code>
 * Key is decomposed as:
 * <code>&lt;rowlength> &lt;row> &lt;columnfamilylength> &lt;columnfamily> &lt;columnqualifier> &lt;timestamp> &lt;keytype></code>
 * Rowlength maximum is Short.MAX_SIZE, column family length maximum is
 * Byte.MAX_SIZE, and column qualifier + key length must be < Integer.MAX_SIZE.
 * The column does not contain the family/qualifier delimiter.
 * 
 * <p>TODO: Group Key-only compartors and operations into a Key class, just
 * for neatness sake, if can figure what to call it.
 */
public class KeyValue {
  static final Log LOG = LogFactory.getLog(KeyValue.class);

  /**
   * Colon character in UTF-8
   */
  public static final char COLUMN_FAMILY_DELIMITER = ':';
  
  /**
   * Comparator for plain key/values; i.e. non-catalog table key/values.
   */
  public static KVComparator COMPARATOR = new KVComparator();

  /**
   * Comparator for plain key; i.e. non-catalog table key.  Works on Key portion
   * of KeyValue only.
   */
  public static KeyComparator KEY_COMPARATOR = new KeyComparator();

  /**
   * A {@link KVComparator} for <code>.META.</code> catalog table
   * {@link KeyValue}s.
   */
  public static KVComparator META_COMPARATOR = new MetaComparator();

  /**
   * A {@link KVComparator} for <code>.META.</code> catalog table
   * {@link KeyValue} keys.
   */
  public static KeyComparator META_KEY_COMPARATOR = new MetaKeyComparator();

  /**
   * A {@link KVComparator} for <code>-ROOT-</code> catalog table
   * {@link KeyValue}s.
   */
  public static KVComparator ROOT_COMPARATOR = new RootComparator();

  /**
   * A {@link KVComparator} for <code>-ROOT-</code> catalog table
   * {@link KeyValue} keys.
   */
  public static KeyComparator ROOT_KEY_COMPARATOR = new RootKeyComparator();

  /**
   * Comparator that compares the family portion of columns only.
   * Use this making NavigableMaps of Stores or when you need to compare
   * column family portion only of two column names.
   */
  public static final RawComparator<byte []> FAMILY_COMPARATOR =
    new RawComparator<byte []> () {
      public int compare(byte [] a, int ao, int al, byte [] b, int bo, int bl) {
        int indexa = KeyValue.getDelimiter(a, ao, al, COLUMN_FAMILY_DELIMITER);
        if (indexa < 0) {
          indexa = al;
        }
        int indexb = KeyValue.getDelimiter(b, bo, bl, COLUMN_FAMILY_DELIMITER);
        if (indexb < 0) {
          indexb = bl;
        }
        return Bytes.compareTo(a, ao, indexa, b, bo, indexb);
      }

      public int compare(byte[] a, byte[] b) {
        return compare(a, 0, a.length, b, 0, b.length);
      }
    };

  // Size of the timestamp and type byte on end of a key -- a long + a byte.
  private static final int TIMESTAMP_TYPE_SIZE =
    Bytes.SIZEOF_LONG /* timestamp */ +
    Bytes.SIZEOF_BYTE /*keytype*/;

  // Size of the length shorts and bytes in key.
  private static final int KEY_INFRASTRUCTURE_SIZE =
    Bytes.SIZEOF_SHORT /*rowlength*/ +
    Bytes.SIZEOF_BYTE /*columnfamilylength*/ +
    TIMESTAMP_TYPE_SIZE;

  // How far into the key the row starts at. First thing to read is the short
  // that says how long the row is.
  private static final int ROW_OFFSET =
    Bytes.SIZEOF_INT /*keylength*/ +
    Bytes.SIZEOF_INT /*valuelength*/;

  // Size of the length ints in a KeyValue datastructure.
  private static final int KEYVALUE_INFRASTRUCTURE_SIZE = ROW_OFFSET;

  /**
   * Key type.
   * Has space for other key types to be added later.  Cannot rely on
   * enum ordinals . They change if item is removed or moved.  Do our own codes.
   */
  public static enum Type {
    Put((byte)4),
    Delete((byte)8),
    DeleteColumn((byte)16),
    DeleteFamily((byte)32),
    // Maximum is used when searching; you look from maximum on down.
    Maximum((byte)255);
    
    private final byte code;
    
    Type(final byte c) {
      this.code = c;
    }
    
    public byte getCode() {
      return this.code;
    }

    /**
     * Cannot rely on enum ordinals . They change if item is removed or moved.
     * Do our own codes.
     * @param b
     * @return Type associated with passed code.
     */
    public static Type codeToType(final byte b) {
      // This is messy repeating each type here below but no way around it; we
      // can't use the enum ordinal.
      if (b == Put.getCode()) {
        return Put;
      } else if (b == Delete.getCode()) {
        return Delete;
      } else if (b == DeleteColumn.getCode()) {
        return DeleteColumn;
      } else if (b == DeleteFamily.getCode()) {
        return DeleteFamily;
      } else if (b == Maximum.getCode()) {
        return Maximum;
      }
      throw new RuntimeException("Unknown code " + b);
    }
  }

  /**
   * Lowest possible key.
   * Makes a Key with highest possible Timestamp, empty row and column.  No
   * key can be equal or lower than this one in memcache or in store file.
   */
  public static final KeyValue LOWESTKEY = 
    new KeyValue(HConstants.EMPTY_BYTE_ARRAY, HConstants.LATEST_TIMESTAMP);
  
  private final byte [] bytes;
  private final int offset;
  private final int length;

  /**
   * Creates a KeyValue from the start of the specified byte array.
   * Presumes <code>bytes</code> content is formatted as a KeyValue blob.
   * @param bytes byte array
   */
  public KeyValue(final byte [] bytes) {
    this(bytes, 0);
  }
  
  /**
   * Creates a KeyValue from the specified byte array and offset.
   * Presumes <code>bytes</code> content starting at <code>offset</code> is
   * formatted as a KeyValue blob.
   * @param bytes byte array
   * @param offset offset to start of KeyValue
   */
  public KeyValue(final byte [] bytes, final int offset) {
    this(bytes, offset, getLength(bytes, offset));
  }

  /**
   * Creates a KeyValue from the specified byte array, starting at offset, and
   * for length <code>length</code>.
   * @param bytes byte array
   * @param offset offset to start of the KeyValue
   * @param length length of the KeyValue
   */
  public KeyValue(final byte [] bytes, final int offset, final int length) {
    this.bytes = bytes;
    this.offset = offset;
    this.length = length;
  }
  
  /**
   * @param row
   * @param timestamp
   * @return KeyValue structure filled with specified values.
   * @throws IllegalArgumentException
   */
  public KeyValue(final String row, final long timestamp) {
    this(Bytes.toBytes(row), timestamp);
  }

  /**
   * @param row
   * @param timestamp
   * @return KeyValue structure filled with specified values.
   * @throws IllegalArgumentException
   */
  public KeyValue(final byte [] row, final long timestamp) {
    this(row, null, timestamp, Type.Put, null);
  }

  /**
   * @param row
   * @param column Column with delimiter between family and qualifier
   * @return KeyValue structure filled with specified values.
   * @throws IllegalArgumentException
   */
  public KeyValue(final String row, final String column) {
    this(row, column, null);
  }

  /**
   * @param row
   * @param column Column with delimiter between family and qualifier
   * @return KeyValue structure filled with specified values.
   * @throws IllegalArgumentException
   */
  public KeyValue(final byte [] row, final byte [] column) {
    this(row, column, null);
  }

  /**
   * @param row
   * @param column Column with delimiter between family and qualifier
   * @param value
   * @return KeyValue structure filled with specified values.
   * @throws IllegalArgumentException
   */
  public KeyValue(final String row, final String column, final byte [] value) {
    this(Bytes.toBytes(row), Bytes.toBytes(column), value);
  }

  /**
   * @param row
   * @param column Column with delimiter between family and qualifier
   * @param value
   * @return KeyValue structure filled with specified values.
   * @throws IllegalArgumentException
   */
  public KeyValue(final byte [] row, final byte [] column, final byte [] value) {
    this(row, column, HConstants.LATEST_TIMESTAMP, value);
  }


  /**
   * @param row
   * @param column Column with delimiter between family and qualifier
   * @param ts
   * @return KeyValue structure filled with specified values.
   * @throws IllegalArgumentException
   */
  public KeyValue(final String row, final String column, final long ts) {
    this(row, column, ts, null);
  }

  /**
   * @param row
   * @param column Column with delimiter between family and qualifier
   * @param ts
   * @return KeyValue structure filled with specified values.
   * @throws IllegalArgumentException
   */
  public KeyValue(final byte [] row, final byte [] column, final long ts) {
    this(row, column, ts, Type.Put);
  }

  /**
   * @param row
   * @param column Column with delimiter between family and qualifier
   * @param timestamp
   * @param value
   * @return KeyValue structure filled with specified values.
   * @throws IllegalArgumentException
   */
  public KeyValue(final String row, final String column,
    final long timestamp, final byte [] value) {
    this(Bytes.toBytes(row),
      column == null? HConstants.EMPTY_BYTE_ARRAY: Bytes.toBytes(column),
      timestamp, value);
  }

  /**
   * @param row
   * @param column Column with delimiter between family and qualifier
   * @param timestamp
   * @param value
   * @return KeyValue structure filled with specified values.
   * @throws IllegalArgumentException
   */
  public KeyValue(final byte [] row, final byte [] column,
     final long timestamp, final byte [] value) {
    this(row, column, timestamp, Type.Put, value);
  }

  /**
   * @param row
   * @param column Column with delimiter between family and qualifier
   * @param timestamp
   * @param type
   * @param value
   * @return KeyValue structure filled with specified values.
   * @throws IllegalArgumentException
   */
  public KeyValue(final String row, final String column,
     final long timestamp, final Type type, final byte [] value) {
    this(Bytes.toBytes(row), Bytes.toBytes(column), timestamp, type,
      value);
  }

  /**
   * @param row
   * @param column Column with delimiter between family and qualifier
   * @param timestamp
   * @param type
   * @return KeyValue structure filled with specified values.
   * @throws IllegalArgumentException
   */
  public KeyValue(final byte [] row, final byte [] column,
      final long timestamp, final Type type) {
    this(row, 0, row.length, column, 0, column == null? 0: column.length,
      timestamp, type, null, 0, -1);
  }

  /**
   * @param row
   * @param column Column with delimiter between family and qualifier
   * @param timestamp
   * @param type
   * @param value
   * @return KeyValue structure filled with specified values.
   * @throws IllegalArgumentException
   */
  public KeyValue(final byte [] row, final byte [] column,
      final long timestamp, final Type type, final byte [] value) {
    this(row, 0, row.length, column, 0, column == null? 0: column.length,
      timestamp, type, value, 0, value == null? 0: value.length);
  }

  /**
   * @param row
   * @param roffset
   * @param rlength
   * @param column Column with delimiter between family and qualifier
   * @param coffset Where to start reading the column.
   * @param clength How long column is (including the family/qualifier delimiter.
   * @param timestamp
   * @param type
   * @param value
   * @param voffset
   * @param vlength
   * @return KeyValue
   * @throws IllegalArgumentException
   */
  public KeyValue(final byte [] row, final int roffset, final int rlength,
      final byte [] column, final int coffset, int clength,
      final long timestamp, final Type type,
      final byte [] value, final int voffset, int vlength) {
    this.bytes = createByteArray(row, roffset, rlength, column, coffset,
      clength, timestamp, type, value, voffset, vlength);
    this.length = bytes.length;
    this.offset = 0;
  }

  /**
   * Write KeyValue format into a byte array.
   * @param row
   * @param roffset
   * @param rlength
   * @param column
   * @param coffset
   * @param clength
   * @param timestamp
   * @param type
   * @param value
   * @param voffset
   * @param vlength
   * @return
   */
  static byte [] createByteArray(final byte [] row, final int roffset,
        final int rlength,
      final byte [] column, final int coffset, int clength,
      final long timestamp, final Type type,
      final byte [] value, final int voffset, int vlength) {
    if (rlength > Short.MAX_VALUE) {
      throw new IllegalArgumentException("Row > " + Short.MAX_VALUE);
    }
    if (row == null) {
      throw new IllegalArgumentException("Row is null");
    }
    // If column is non-null, figure where the delimiter is at.
    int delimiteroffset = 0;
    if (column != null && column.length > 0) {
      delimiteroffset = getFamilyDelimiterIndex(column, coffset, clength);
      if (delimiteroffset > Byte.MAX_VALUE) {
        throw new IllegalArgumentException("Family > " + Byte.MAX_VALUE);
      }
    }
    // Value length
    vlength = value == null? 0: vlength;
    // Column length - minus delimiter
    clength = column == null || column.length == 0? 0: clength - 1;
    long longkeylength = KEY_INFRASTRUCTURE_SIZE + rlength + clength;
    if (longkeylength > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("keylength " + longkeylength + " > " +
        Integer.MAX_VALUE);
    }
    int keylength = (int)longkeylength;
    // Allocate right-sized byte array.
    byte [] bytes = new byte[KEYVALUE_INFRASTRUCTURE_SIZE + keylength + vlength];
    // Write key, value and key row length.
    int pos = 0;
    pos = Bytes.putInt(bytes, pos, keylength);
    pos = Bytes.putInt(bytes, pos, vlength);
    pos = Bytes.putShort(bytes, pos, (short)(rlength & 0x0000ffff));
    pos = Bytes.putBytes(bytes, pos, row, roffset, rlength);
    // Write out column family length.
    pos = Bytes.putByte(bytes, pos, (byte)(delimiteroffset & 0x0000ff));
    if (column != null && column.length != 0) {
      // Write family.
      pos = Bytes.putBytes(bytes, pos, column, coffset, delimiteroffset);
      // Write qualifier.
      delimiteroffset++;
      pos = Bytes.putBytes(bytes, pos, column, coffset + delimiteroffset,
        column.length - delimiteroffset);
    }
    pos = Bytes.putLong(bytes, pos, timestamp);
    pos = Bytes.putByte(bytes, pos, type.getCode());
    if (value != null && value.length > 0) {
      pos = Bytes.putBytes(bytes, pos, value, voffset, vlength);
    }
    return bytes;
  }

  // Needed doing 'contains' on List.
  public boolean equals(Object other) {
    KeyValue kv = (KeyValue)other;
    // Comparing bytes should be fine doing equals test.  Shouldn't have to
    // worry about special .META. comparators doing straight equals.
    boolean result = Bytes.BYTES_RAWCOMPARATOR.compare(getBuffer(),
        getKeyOffset(), getKeyLength(),
      kv.getBuffer(), kv.getKeyOffset(), kv.getKeyLength()) == 0;
    return result;
  }

  /**
   * @param timestamp
   * @return Clone of bb's key portion with only the row and timestamp filled in.
   * @throws IOException
   */
  public KeyValue cloneRow(final long timestamp) {
    return new KeyValue(getBuffer(), getRowOffset(), getRowLength(),
      null, 0, 0, timestamp, Type.codeToType(getType()), null, 0, 0);
  }

  /**
   * @return Clone of bb's key portion with type set to Type.Delete.
   * @throws IOException
   */
  public KeyValue cloneDelete() {
    return createKey(Type.Delete);
  }

  /**
   * @return Clone of bb's key portion with type set to Type.Maximum. Use this
   * doing lookups where you are doing getClosest.  Using Maximum, you'll be
   * sure to trip over all of the other key types since Maximum sorts first.
   * @throws IOException
   */
  public KeyValue cloneMaximum() {
     return createKey(Type.Maximum);
  }

  /*
   * Make a clone with the new type.
   * Does not copy value.
   * @param newtype New type to set on clone of this key.
   * @return Clone of this key with type set to <code>newtype</code>
   */
  private KeyValue createKey(final Type newtype) {
    int keylength= getKeyLength();
    int l = keylength + ROW_OFFSET;
    byte [] other = new byte[l];
    System.arraycopy(getBuffer(), getOffset(), other, 0, l);
    // Set value length to zero.
    Bytes.putInt(other, Bytes.SIZEOF_INT, 0);
    // Set last byte, the type, to new type
    other[l - 1] = newtype.getCode();
    return new KeyValue(other, 0, other.length);
  }

  public String toString() {
    return keyToString(this.bytes, this.offset + ROW_OFFSET, getKeyLength()) +
      "/vlen=" + getValueLength();
  }

  /**
   * @param b Key portion of a KeyValue.
   * @return Key as a String.
   */
  public static String keyToString(final byte [] k) {
    return keyToString(k, 0, k.length);
  }

  /**
   * @param b Key portion of a KeyValue.
   * @param o Offset to start of key
   * @param l Length of key.
   * @return Key as a String.
   */
  public static String keyToString(final byte [] b, final int o, final int l) {
    int rowlength = Bytes.toShort(b, o);
    String row = Bytes.toString(b, o + Bytes.SIZEOF_SHORT, rowlength);
    int columnoffset = o + Bytes.SIZEOF_SHORT + 1 + rowlength;
    int familylength = b[columnoffset - 1];
    int columnlength = l - ((columnoffset - o) + TIMESTAMP_TYPE_SIZE);
    String family = familylength == 0? "":
      Bytes.toString(b, columnoffset, familylength);
    String qualifier = columnlength == 0? "":
      Bytes.toString(b, columnoffset + familylength,
      columnlength - familylength);
    long timestamp = Bytes.toLong(b, o + (l - TIMESTAMP_TYPE_SIZE));
    byte type = b[o + l - 1];
    return row + "/" + family +
      (family != null && family.length() > 0? COLUMN_FAMILY_DELIMITER: "") +
      qualifier + "/" + timestamp + "/" + Type.codeToType(type);
  }

  /**
   * @return The byte array backing this KeyValue.
   */
  public byte [] getBuffer() {
    return this.bytes;
  }

  /**
   * @return Offset into {@link #getBuffer()} at which this KeyValue starts.
   */
  public int getOffset() {
    return this.offset;
  }

  /**
   * @return Length of bytes this KeyValue occupies in {@link #getBuffer()}.
   */
  public int getLength() {
    return length;
  }

  /*
   * Determines the total length of the KeyValue stored in the specified
   * byte array and offset.  Includes all headers.
   * @param bytes byte array
   * @param offset offset to start of the KeyValue
   * @return length of entire KeyValue, in bytes
   */
  private static int getLength(byte [] bytes, int offset) {
    return (2 * Bytes.SIZEOF_INT) + 
        Bytes.toInt(bytes, offset) +
        Bytes.toInt(bytes, offset + Bytes.SIZEOF_INT);
  }

  /**
   * @return Copy of the key portion only.  Used compacting and testing.
   */
  public byte [] getKey() {
    int keylength = getKeyLength();
    byte [] key = new byte[keylength];
    System.arraycopy(getBuffer(), ROW_OFFSET, key, 0, keylength);
    return key;
  }

  /**
   * @return Key offset in backing buffer..
   */
  public int getKeyOffset() {
    return this.offset + ROW_OFFSET;
  }

  /**
   * @return Row length.
   */
  public short getRowLength() {
    return Bytes.toShort(this.bytes, getKeyOffset());
  }

  /**
   * @return Offset into backing buffer at which row starts.
   */
  public int getRowOffset() {
    return getKeyOffset() + Bytes.SIZEOF_SHORT;
  }

  /**
   * Do not use this unless you have to.
   * Use {@link #getBuffer()} with appropriate offsets and lengths instead.
   * @return Row in a new byte array.
   */
  public byte [] getRow() {
    int o = getRowOffset();
    short l = getRowLength();
    byte [] result = new byte[l];
    System.arraycopy(getBuffer(), o, result, 0, l);
    return result;
  }

  /**
   * @return Timestamp
   */
  public long getTimestamp() {
    return getTimestamp(getKeyLength());
  }

  /**
   * @param keylength Pass if you have it to save on a int creation.
   * @return Timestamp
   */
  long getTimestamp(final int keylength) {
    int tsOffset = getTimestampOffset(keylength);
    return Bytes.toLong(this.bytes, tsOffset);
  }

  /**
   * @param keylength Pass if you have it to save on a int creation.
   * @return Offset into backing buffer at which timestamp starts.
   */
  int getTimestampOffset(final int keylength) {
    return getKeyOffset() + keylength - TIMESTAMP_TYPE_SIZE;
  }

  /**
   * @return True if a {@link Type#Delete}.
   */
  public boolean isDeleteType() {
    return getType() == Type.Delete.getCode();
  }

  /**
   * @return Type of this KeyValue.
   */
  byte getType() {
    return getType(getKeyLength());
  }

  /**
   * @param keylength Pass if you have it to save on a int creation.
   * @return Type of this KeyValue.
   */
  byte getType(final int keylength) {
    return this.bytes[this.offset + keylength - 1 + ROW_OFFSET];
  }

  /**
   * @return Length of key portion.
   */
  public int getKeyLength() {
    return Bytes.toInt(this.bytes, this.offset);
  }

  /**
   * @return Value length
   */
  public int getValueLength() {
    return Bytes.toInt(this.bytes, this.offset + Bytes.SIZEOF_INT);
  }

  /**
   * @return Offset into backing buffer at which value starts.
   */
  public int getValueOffset() {
    return getKeyOffset() + getKeyLength();
  }

  /**
   * Do not use unless you have to.  Use {@link #getBuffer()} with appropriate
   * offset and lengths instead.
   * @return Value in a new byte array.
   */
  public byte [] getValue() {
    int o = getValueOffset();
    int l = getValueLength();
    byte [] result = new byte[l];
    System.arraycopy(getBuffer(), o, result, 0, l);
    return result;
  }

  /**
   * @return Offset into backing buffer at which the column begins
   */
  public int getColumnOffset() {
    return getColumnOffset(getRowLength());
  }

  /**
   * @param rowlength Pass if you have it to save on an int creation.
   * @return Offset into backing buffer at which the column begins
   */
  public int getColumnOffset(final int rowlength) {
    return getRowOffset() + rowlength + 1;
  }

  /**
   * @param columnoffset Pass if you have it to save on an int creation.
   * @return Length of family portion of column.
   */
  int getFamilyLength(final int columnoffset) {
    return this.bytes[columnoffset - 1];
  }

  /**
   * @param columnoffset Pass if you have it to save on an int creation.
   * @return Length of column.
   */
  public int getColumnLength(final int columnoffset) {
    return getColumnLength(columnoffset, getKeyLength());
  }

  int getColumnLength(final int columnoffset, final int keylength) {
    return (keylength + ROW_OFFSET) - (columnoffset - this.offset) -
      TIMESTAMP_TYPE_SIZE;
  }

  /**
   * @param family
   * @return True if matching families.
   */
  public boolean matchingFamily(final byte [] family) {
    int o = getColumnOffset();
    // Family length byte is just before the column starts.
    int l = this.bytes[o - 1];
    return Bytes.compareTo(family, 0, family.length, this.bytes, o, l) == 0;
  }

  /**
   * @param column Column minus its delimiter
   * @return True if column matches.
   * @see #matchingColumn(byte[])
   */
  public boolean matchingColumnNoDelimiter(final byte [] column) {
    int o = getColumnOffset();
    int l = getColumnLength(o);
    return compareColumns(getBuffer(), o, l, column, 0, column.length) == 0;
  }

  /**
   * @param column Column with delimiter
   * @return True if column matches.
   */
  public boolean matchingColumn(final byte [] column) {
    int index = getFamilyDelimiterIndex(column, 0, column.length);
    int o = getColumnOffset();
    int l = getColumnLength(o);
    int result = Bytes.compareTo(getBuffer(), o, index, column, 0, index);
    if (result != 0) {
      return false;
    }
    return Bytes.compareTo(getBuffer(), o + index, l - index,
      column, index + 1, column.length - (index + 1)) == 0;
  }

  /**
   * @param left
   * @param loffset
   * @param llength
   * @param right
   * @param roffset
   * @param rlength
   * @return
   */
  static int compareColumns(final byte [] left, final int loffset,
      final int llength, final byte [] right, final int roffset,
      final int rlength) {
    return Bytes.compareTo(left, loffset, llength, right, roffset, rlength);
  }

  /**
   * @return True if non-null row and column.
   */
  public boolean nonNullRowAndColumn() {
    return getRowLength() > 0 && !isEmptyColumn();
  }

  /**
   * @return Returns column String with delimiter added back. Expensive!
   */
  public String getColumnString() {
    int o = getColumnOffset();
    int l = getColumnLength(o);
    int familylength = getFamilyLength(o);
    return Bytes.toString(this.bytes, o, familylength) +
      COLUMN_FAMILY_DELIMITER + Bytes.toString(this.bytes,
       o + familylength, l - familylength);
  }

  /**
   * Do not use this unless you have to.
   * Use {@link #getBuffer()} with appropriate offsets and lengths instead.
   * @return Returns column. Makes a copy.  Inserts delimiter.
   */
  public byte [] getColumn() {
    int o = getColumnOffset();
    int l = getColumnLength(o);
    int familylength = getFamilyLength(o);
    byte [] result = new byte[l + 1];
    System.arraycopy(getBuffer(), o, result, 0, familylength);
    result[familylength] = COLUMN_FAMILY_DELIMITER;
    System.arraycopy(getBuffer(), o + familylength, result,
      familylength + 1, l - familylength);
    return result;
  }

  /**
   * @return True if column is empty.
   */
  public boolean isEmptyColumn() {
    return getColumnLength(getColumnOffset()) == 0;
  }

  /**
   * @param b
   * @return Index of the family-qualifier colon delimiter character in passed
   * buffer.
   */
  public static int getFamilyDelimiterIndex(final byte [] b, final int offset,
      final int length) {
    return getRequiredDelimiter(b, offset, length, COLUMN_FAMILY_DELIMITER);
  }

  private static int getRequiredDelimiter(final byte [] b,
      final int offset, final int length, final int delimiter) {
    int index = getDelimiter(b, offset, length, delimiter);
    if (index < 0) {
      throw new IllegalArgumentException("No " + (char)delimiter + " in <" +
        Bytes.toString(b) + ">" + ", length=" + length + ", offset=" + offset);
    }
    return index;
  }

  static int getRequiredDelimiterInReverse(final byte [] b,
      final int offset, final int length, final int delimiter) {
    int index = getDelimiterInReverse(b, offset, length, delimiter);
    if (index < 0) {
      throw new IllegalArgumentException("No " + delimiter + " in <" +
        Bytes.toString(b) + ">" + ", length=" + length + ", offset=" + offset);
    }
    return index;
  }

  /*
   * @param b
   * @param delimiter
   * @return Index of delimiter having started from end of <code>b</code> moving
   * leftward.
   */
  static int getDelimiter(final byte [] b, int offset, final int length,
      final int delimiter) {
    if (b == null) {
      throw new NullPointerException();
    }
    int result = -1;
    for (int i = offset; i < length + offset; i++) {
      if (b[i] == delimiter) {
        result = i;
        break;
      }
    }
    return result;
  }

  /*
   * @param b
   * @param delimiter
   * @return Index of delimiter
   */
  static int getDelimiterInReverse(final byte [] b, final int offset,
      final int length, final int delimiter) {
    if (b == null) {
      throw new NullPointerException();
    }
    int result = -1;
    for (int i = (offset + length) - 1; i >= offset; i--) {
      if (b[i] == delimiter) {
        result = i;
        break;
      }
    }
    return result;
  }

  /**
   * A {@link KVComparator} for <code>-ROOT-</code> catalog table
   * {@link KeyValue}s.
   */
  public static class RootComparator extends MetaComparator {
    private final KeyComparator rawcomparator = new RootKeyComparator();
    
    public KeyComparator getRawComparator() {
      return this.rawcomparator;
    }
  }

  /**
   * A {@link KVComparator} for <code>.META.</code> catalog table
   * {@link KeyValue}s.
   */
  public static class MetaComparator extends KVComparator {
    private final KeyComparator rawcomparator = new MetaKeyComparator();

    public KeyComparator getRawComparator() {
      return this.rawcomparator;
    }
  }

  /**
   * Compare KeyValues.
   * Hosts a {@link KeyComparator}.
   */
  public static class KVComparator implements java.util.Comparator<KeyValue> {
    private final KeyComparator rawcomparator = new KeyComparator();

    /**
     * @return RawComparator that can compare the Key portion of a KeyValue.
     * Used in hfile where indices are the Key portion of a KeyValue.
     */
    public KeyComparator getRawComparator() {
      return this.rawcomparator;
    }

    public int compare(final KeyValue left, final KeyValue right) {
      return getRawComparator().compare(left.getBuffer(),
          left.getOffset() + ROW_OFFSET, left.getKeyLength(),
        right.getBuffer(), right.getOffset() + ROW_OFFSET,
          right.getKeyLength());
    }

    public int compareTimestamps(final KeyValue left, final KeyValue right) {
      return compareTimestamps(left, left.getKeyLength(), right,
        right.getKeyLength());
    }

    int compareTimestamps(final KeyValue left, final int lkeylength,
        final KeyValue right, final int rkeylength) {
      // Compare timestamps
      long ltimestamp = left.getTimestamp(lkeylength);
      long rtimestamp = right.getTimestamp(rkeylength);
      return getRawComparator().compareTimestamps(ltimestamp, rtimestamp);
    }

    /**
     * @param left
     * @param right
     * @return Result comparing rows.
     */
    public int compareRows(final KeyValue left, final KeyValue right) {
      return compareRows(left, left.getRowLength(), right, right.getRowLength());
    }

    /**
     * @param left
     * @param lrowlength Length of left row.
     * @param right
     * @param rrowlength Length of right row.
     * @return Result comparing rows.
     */
    public int compareRows(final KeyValue left, final short lrowlength,
        final KeyValue right, final short rrowlength) {
      return getRawComparator().compareRows(left.getBuffer(),
          left.getRowOffset(), lrowlength,
        right.getBuffer(), right.getRowOffset(), rrowlength);
    }

    /**
     * @param left
     * @param row
     * @return
     */
    public int compareRows(final KeyValue left, final byte [] row) {
      return getRawComparator().compareRows(left.getBuffer(),
          left.getRowOffset(), left.getRowLength(), row, 0, row.length);
    }

    public int compareRows(byte [] left, int loffset, int llength,
        byte [] right, int roffset, int rlength) {
      return getRawComparator().compareRows(left, loffset, llength,
        right, roffset, rlength);
    }

    public int compareColumns(final KeyValue left, final byte [] right,
        final int roffset, final int rlength) {
      int offset = left.getColumnOffset();
      int length = left.getColumnLength(offset);
      return getRawComparator().compareColumns(left.getBuffer(), offset, length,
        right, roffset, rlength);
    }

    int compareColumns(final KeyValue left, final short lrowlength,
        final int lkeylength, final KeyValue right, final short rrowlength,
        final int rkeylength) {
      int loffset = left.getColumnOffset(lrowlength);
      int roffset = right.getColumnOffset(rrowlength);
      int llength = left.getColumnLength(loffset, lkeylength);
      int rlength = right.getColumnLength(roffset, rkeylength);
      return getRawComparator().compareColumns(left.getBuffer(), loffset,
          llength,
        right.getBuffer(), roffset, rlength);
    }

    /**
     * Compares the row and column of two keyvalues
     * @param left
     * @param right
     * @return True if same row and column.
     */
    public boolean matchingRowColumn(final KeyValue left,
        final KeyValue right) {
      short lrowlength = left.getRowLength();
      short rrowlength = right.getRowLength();
      if (!matchingRows(left, lrowlength, right, rrowlength)) {
        return false;
      }
      int lkeylength = left.getKeyLength();
      int rkeylength = right.getKeyLength();
      return compareColumns(left, lrowlength, lkeylength,
        right, rrowlength, rkeylength) == 0;
    }

    /**
     * @param left
     * @param right
     * @return True if rows match.
     */
    public boolean matchingRows(final KeyValue left, final byte [] right) {
      return compareRows(left, right) == 0;
    }

    /**
     * @param left
     * @param right
     * @return True if rows match.
     */
    public boolean matchingRows(final KeyValue left, final KeyValue right) {
      short lrowlength = left.getRowLength();
      short rrowlength = right.getRowLength();
      return matchingRows(left, lrowlength, right, rrowlength);
    }

    /**
     * @param left
     * @param lrowlength
     * @param right
     * @param rrowlength
     * @return True if rows match.
     */
    public boolean matchingRows(final KeyValue left, final short lrowlength,
        final KeyValue right, final short rrowlength) {
      int compare = compareRows(left, lrowlength, right, rrowlength);
      if (compare != 0) {
        return false;
      }
      return true;
    }

    public boolean matchingRows(final byte [] left, final int loffset,
        final int llength,
        final byte [] right, final int roffset, final int rlength) {
      int compare = compareRows(left, loffset, llength, right, roffset, rlength);
      if (compare != 0) {
        return false;
      }
      return true;
    }

    /**
     * Compares the row and timestamp of two keys
     * Was called matchesWithoutColumn in HStoreKey.
     * @param right Key to compare against.
     * @return True if same row and timestamp is greater than the timestamp in
     * <code>right</code>
     */
    public boolean matchingRowsGreaterTimestamp(final KeyValue left,
        final KeyValue right) {
      short lrowlength = left.getRowLength();
      short rrowlength = right.getRowLength();
      if (!matchingRows(left, lrowlength, right, rrowlength)) {
        return false;
      }
      return left.getTimestamp() >= right.getTimestamp();
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
      return new KVComparator();
    }
 
    /**
     * @return Comparator that ignores timestamps; useful counting versions.
     * @throws IOException
     */
    public KVComparator getComparatorIgnoringTimestamps() {
      KVComparator c = null;
      try {
        c = (KVComparator)this.clone();
        c.getRawComparator().ignoreTimestamp = true;
      } catch (CloneNotSupportedException e) {
        LOG.error("Not supported", e);
      }
      return c;
    }

    /**
     * @return Comparator that ignores key type; useful checking deletes
     * @throws IOException
     */
    public KVComparator getComparatorIgnoringType() {
      KVComparator c = null;
      try {
        c = (KVComparator)this.clone();
        c.getRawComparator().ignoreType = true;
      } catch (CloneNotSupportedException e) {
        LOG.error("Not supported", e);
      }
      return c;
    }
  }

  /**
   * @param row
   * @return First possible KeyValue on passed <code>row</code>
   */
  public static KeyValue createFirstOnRow(final byte [] row) {
    return createFirstOnRow(row, HConstants.LATEST_TIMESTAMP);
  }

  /**
   * @param row
   * @param ts
   * @return First possible key on passed <code>row</code> and timestamp.
   */
  public static KeyValue createFirstOnRow(final byte [] row,
      final long ts) {
    return createFirstOnRow(row, null, ts);
  }

  /**
   * @param row
   * @param ts
   * @return First possible key on passed <code>row</code>, column and timestamp.
   */
  public static KeyValue createFirstOnRow(final byte [] row, final byte [] c,
      final long ts) {
    return new KeyValue(row, c, ts, Type.Maximum);
  }

  /**
   * @param b
   * @param o
   * @param l
   * @return A KeyValue made of a byte array that holds the key-only part.
   * Needed to convert hfile index members to KeyValues.
   */
  public static KeyValue createKeyValueFromKey(final byte [] b, final int o,
      final int l) {
    byte [] newb = new byte[b.length + ROW_OFFSET];
    System.arraycopy(b, o, newb, ROW_OFFSET, l);
    Bytes.putInt(newb, 0, b.length);
    Bytes.putInt(newb, Bytes.SIZEOF_INT, 0);
    return new KeyValue(newb);
  }

  /**
   * Compare key portion of a {@link KeyValue} for keys in <code>-ROOT-<code>
   * table.
   */
  public static class RootKeyComparator extends MetaKeyComparator {
    public int compareRows(byte [] left, int loffset, int llength,
        byte [] right, int roffset, int rlength) {
      // Rows look like this: .META.,ROW_FROM_META,RID
      //        LOG.info("ROOT " + Bytes.toString(left, loffset, llength) +
      //          "---" + Bytes.toString(right, roffset, rlength));
      final int metalength = 7; // '.META.' length
      int lmetaOffsetPlusDelimiter = loffset + metalength;
      int leftFarDelimiter = getDelimiterInReverse(left, lmetaOffsetPlusDelimiter,
          llength - metalength, HRegionInfo.DELIMITER);
      int rmetaOffsetPlusDelimiter = roffset + metalength;
      int rightFarDelimiter = getDelimiterInReverse(right,
          rmetaOffsetPlusDelimiter, rlength - metalength,
          HRegionInfo.DELIMITER);
      if (leftFarDelimiter < 0 && rightFarDelimiter >= 0) {
        // Nothing between .META. and regionid.  Its first key.
        return -1;
      } else if (rightFarDelimiter < 0 && leftFarDelimiter >= 0) {
        return 1;
      } else if (leftFarDelimiter < 0 && rightFarDelimiter < 0) {
        return 0;
      }
      int result = super.compareRows(left, lmetaOffsetPlusDelimiter,
          leftFarDelimiter - lmetaOffsetPlusDelimiter,
          right, rmetaOffsetPlusDelimiter,
          rightFarDelimiter - rmetaOffsetPlusDelimiter);
      if (result != 0) {
        return result;
      }
      // Compare last part of row, the rowid.
      leftFarDelimiter++;
      rightFarDelimiter++;
      result = compareRowid(left, leftFarDelimiter,
          llength - (leftFarDelimiter - loffset),
          right, rightFarDelimiter, rlength - (rightFarDelimiter - roffset));
      return result;
    }
  }

  /**
   * Compare key portion of a {@link KeyValue} for keys in <code>.META.</code>
   * table.
   */
  public static class MetaKeyComparator extends KeyComparator {
    public int compareRows(byte [] left, int loffset, int llength,
        byte [] right, int roffset, int rlength) {
      //        LOG.info("META " + Bytes.toString(left, loffset, llength) +
      //          "---" + Bytes.toString(right, roffset, rlength));
      int leftDelimiter = getDelimiter(left, loffset, llength,
          HRegionInfo.DELIMITER);
      int rightDelimiter = getDelimiter(right, roffset, rlength,
          HRegionInfo.DELIMITER);
      if (leftDelimiter < 0 && rightDelimiter >= 0) {
        // Nothing between .META. and regionid.  Its first key.
        return -1;
      } else if (rightDelimiter < 0 && leftDelimiter >= 0) {
        return 1;
      } else if (leftDelimiter < 0 && rightDelimiter < 0) {
        return 0;
      }
      // Compare up to the delimiter
      int result = Bytes.compareTo(left, loffset, leftDelimiter - loffset,
          right, roffset, rightDelimiter - roffset);
      if (result != 0) {
        return result;
      }
      // Compare middle bit of the row.
      // Move past delimiter
      leftDelimiter++;
      rightDelimiter++;
      int leftFarDelimiter = getRequiredDelimiterInReverse(left, leftDelimiter,
          llength - (leftDelimiter - loffset), HRegionInfo.DELIMITER);
      int rightFarDelimiter = getRequiredDelimiterInReverse(right,
          rightDelimiter, rlength - (rightDelimiter - roffset),
          HRegionInfo.DELIMITER);
      // Now compare middlesection of row.
      result = super.compareRows(left, leftDelimiter,
          leftFarDelimiter - leftDelimiter, right, rightDelimiter,
          rightFarDelimiter - rightDelimiter);
      if (result != 0) {
        return result;
      }
      // Compare last part of row, the rowid.
      leftFarDelimiter++;
      rightFarDelimiter++;
      result = compareRowid(left, leftFarDelimiter,
          llength - (leftFarDelimiter - loffset),
          right, rightFarDelimiter, rlength - (rightFarDelimiter - roffset));
      return result;
    }

    protected int compareRowid(byte[] left, int loffset, int llength,
        byte[] right, int roffset, int rlength) {
      return Bytes.compareTo(left, loffset, llength, right, roffset, rlength);
    }
  }

  /**
   * Compare key portion of a {@link KeyValue}
   */
  public static class KeyComparator implements RawComparator<byte []> {
    volatile boolean ignoreTimestamp = false;
    volatile boolean ignoreType = false;

    public int compare(byte[] left, int loffset, int llength, byte[] right,
        int roffset, int rlength) {
      // Compare row
      short lrowlength = Bytes.toShort(left, loffset);
      short rrowlength = Bytes.toShort(right, roffset);
      int compare = compareRows(left, loffset + Bytes.SIZEOF_SHORT,
          lrowlength,
          right, roffset + Bytes.SIZEOF_SHORT, rrowlength);
      if (compare != 0) {
        return compare;
      }

      // Compare column family.  Start compare past row and family length.
      int lcolumnoffset = Bytes.SIZEOF_SHORT + lrowlength + 1 + loffset;
      int rcolumnoffset = Bytes.SIZEOF_SHORT + rrowlength + 1 + roffset;
      int lcolumnlength = llength - TIMESTAMP_TYPE_SIZE -
        (lcolumnoffset - loffset);
      int rcolumnlength = rlength - TIMESTAMP_TYPE_SIZE -
        (rcolumnoffset - roffset);
      compare = Bytes.compareTo(left, lcolumnoffset, lcolumnlength, right,
          rcolumnoffset, rcolumnlength);
      if (compare != 0) {
        return compare;
      }

      if (!this.ignoreTimestamp) {
        // Get timestamps.
        long ltimestamp = Bytes.toLong(left,
            loffset + (llength - TIMESTAMP_TYPE_SIZE));
        long rtimestamp = Bytes.toLong(right,
            roffset + (rlength - TIMESTAMP_TYPE_SIZE));
        compare = compareTimestamps(ltimestamp, rtimestamp);
        if (compare != 0) {
          return compare;
        }
      }

      if (!this.ignoreType) {
        // Compare types. Let the delete types sort ahead of puts; i.e. types
        // of higher numbers sort before those of lesser numbers
        byte ltype = left[loffset + (llength - 1)];
        byte rtype = right[roffset + (rlength - 1)];
        return (0xff & rtype) - (0xff & ltype);
      }
      return 0;
    }

    public int compare(byte[] left, byte[] right) {
      return compare(left, 0, left.length, right, 0, right.length);
    }

    protected int compareRows(byte [] left, int loffset, int llength,
        byte [] right, int roffset, int rlength) {
      return Bytes.compareTo(left, loffset, llength, right, roffset, rlength);
    }

    protected int compareColumns(byte [] left, int loffset, int llength,
        byte [] right, int roffset, int rlength) {
      return KeyValue.compareColumns(left, loffset, llength, right, roffset, rlength);
    }

    int compareTimestamps(final long ltimestamp, final long rtimestamp) {
      // The below older timestamps sorting ahead of newer timestamps looks
      // wrong but it is intentional. This way, newer timestamps are first
      // found when we iterate over a memcache and newer versions are the
      // first we trip over when reading from a store file.
      if (ltimestamp < rtimestamp) {
        return 1;
      } else if (ltimestamp > rtimestamp) {
        return -1;
      }
      return 0;
    }
  }
}