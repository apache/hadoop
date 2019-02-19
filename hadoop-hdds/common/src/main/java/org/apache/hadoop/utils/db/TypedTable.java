/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.hadoop.utils.db;

import java.io.IOException;

/**
 * Strongly typed table implementation.
 * <p>
 * Automatically converts values and keys using a raw byte[] based table
 * implementation and registered converters.
 *
 * @param <KEY>   type of the keys in the store.
 * @param <VALUE> type of the values in the store.
 */
public class TypedTable<KEY, VALUE> implements Table<KEY, VALUE> {

  private Table<byte[], byte[]> rawTable;

  private CodecRegistry codecRegistry;

  private Class<KEY> keyType;

  private Class<VALUE> valueType;

  public TypedTable(
      Table<byte[], byte[]> rawTable,
      CodecRegistry codecRegistry, Class<KEY> keyType,
      Class<VALUE> valueType) {
    this.rawTable = rawTable;
    this.codecRegistry = codecRegistry;
    this.keyType = keyType;
    this.valueType = valueType;
  }

  @Override
  public void put(KEY key, VALUE value) throws IOException {
    byte[] keyData = codecRegistry.asRawData(key);
    byte[] valueData = codecRegistry.asRawData(value);
    rawTable.put(keyData, valueData);
  }

  @Override
  public void putWithBatch(BatchOperation batch, KEY key, VALUE value)
      throws IOException {
    byte[] keyData = codecRegistry.asRawData(key);
    byte[] valueData = codecRegistry.asRawData(value);
    rawTable.putWithBatch(batch, keyData, valueData);
  }

  @Override
  public boolean isEmpty() throws IOException {
    return rawTable.isEmpty();
  }

  @Override
  public VALUE get(KEY key) throws IOException {
    byte[] keyBytes = codecRegistry.asRawData(key);
    byte[] valueBytes = rawTable.get(keyBytes);
    return codecRegistry.asObject(valueBytes, valueType);
  }

  @Override
  public void delete(KEY key) throws IOException {
    rawTable.delete(codecRegistry.asRawData(key));
  }

  @Override
  public void deleteWithBatch(BatchOperation batch, KEY key)
      throws IOException {
    rawTable.deleteWithBatch(batch, codecRegistry.asRawData(key));

  }

  @Override
  public TableIterator<KEY, TypedKeyValue> iterator() {
    TableIterator<byte[], ? extends KeyValue<byte[], byte[]>> iterator =
        rawTable.iterator();
    return new TypedTableIterator(iterator, keyType, valueType);
  }

  @Override
  public String getName() throws IOException {
    return rawTable.getName();
  }

  @Override
  public void close() throws Exception {
    rawTable.close();

  }

  /**
   * Key value implementation for strongly typed tables.
   */
  public class TypedKeyValue implements KeyValue<KEY, VALUE> {

    private KeyValue<byte[], byte[]> rawKeyValue;

    public TypedKeyValue(KeyValue<byte[], byte[]> rawKeyValue) {
      this.rawKeyValue = rawKeyValue;
    }

    public TypedKeyValue(KeyValue<byte[], byte[]> rawKeyValue,
        Class<KEY> keyType, Class<VALUE> valueType) {
      this.rawKeyValue = rawKeyValue;
    }

    @Override
    public KEY getKey() throws IOException {
      return codecRegistry.asObject(rawKeyValue.getKey(), keyType);
    }

    @Override
    public VALUE getValue() throws IOException {
      return codecRegistry.asObject(rawKeyValue.getValue(), valueType);
    }
  }

  /**
   * Table Iterator implementation for strongly typed tables.
   */
  public class TypedTableIterator implements TableIterator<KEY, TypedKeyValue> {

    private TableIterator<byte[], ? extends KeyValue<byte[], byte[]>>
        rawIterator;
    private final Class<KEY> keyClass;
    private final Class<VALUE> valueClass;

    public TypedTableIterator(
        TableIterator<byte[], ? extends KeyValue<byte[], byte[]>> rawIterator,
        Class<KEY> keyType,
        Class<VALUE> valueType) {
      this.rawIterator = rawIterator;
      keyClass = keyType;
      valueClass = valueType;
    }

    @Override
    public void seekToFirst() {
      rawIterator.seekToFirst();
    }

    @Override
    public void seekToLast() {
      rawIterator.seekToLast();
    }

    @Override
    public TypedKeyValue seek(KEY key) throws IOException {
      byte[] keyBytes = codecRegistry.asRawData(key);
      KeyValue<byte[], byte[]> result = rawIterator.seek(keyBytes);
      if (result == null) {
        return null;
      }
      return new TypedKeyValue(result);
    }

    @Override
    public KEY key() throws IOException {
      byte[] result = rawIterator.key();
      if (result == null) {
        return null;
      }
      return codecRegistry.asObject(result, keyClass);
    }

    @Override
    public TypedKeyValue value() {
      KeyValue keyValue = rawIterator.value();
      if(keyValue != null) {
        return new TypedKeyValue(keyValue, keyClass, valueClass);
      }
      return null;
    }

    @Override
    public void close() throws IOException {
      rawIterator.close();
    }

    @Override
    public boolean hasNext() {
      return rawIterator.hasNext();
    }

    @Override
    public TypedKeyValue next() {
      return new TypedKeyValue(rawIterator.next(), keyType,
          valueType);
    }
  }
}
