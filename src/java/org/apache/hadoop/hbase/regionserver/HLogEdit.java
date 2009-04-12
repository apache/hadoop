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
package org.apache.hadoop.hbase.regionserver;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.BatchOperation;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

/**
 * A log value.
 *
 * These aren't sortable; you need to sort by the matching HLogKey.
 * TODO: Remove.  Just output KVs.
 */
public class HLogEdit implements Writable, HConstants {
  /** Value stored for a deleted item */
  public static byte [] DELETED_BYTES;
  /** Value written to HLog on a complete cache flush */
  public static byte [] COMPLETE_CACHE_FLUSH;

  static {
    try {
      DELETED_BYTES = "HBASE::DELETEVAL".getBytes(UTF8_ENCODING);
      COMPLETE_CACHE_FLUSH = "HBASE::CACHEFLUSH".getBytes(UTF8_ENCODING);
    } catch (UnsupportedEncodingException e) {
      assert(false);
    }
  }

  /** If transactional log entry, these are the op codes */
  public enum TransactionalOperation {
    /** start transaction */
    START,
    /** Equivalent to append in non-transactional environment */
    WRITE,
    /** Transaction commit entry */
    COMMIT,
    /** Abort transaction entry */
    ABORT
  }

  private KeyValue kv;
  private static final int MAX_VALUE_LEN = 128;
  
  private boolean isTransactionEntry;
  private Long transactionId = null;
  private TransactionalOperation operation;


  /**
   * Default constructor used by Writable
   */
  public HLogEdit() {
    this(null);
  }

  /**
   * Construct a fully initialized HLogEdit
   * @param kv
   */
  public HLogEdit(final KeyValue kv) {
    this.kv = kv;
    this.isTransactionEntry = false;
  }

  /** 
   * Construct a WRITE transaction. 
   * @param transactionId
   * @param op
   * @param timestamp
   */
  public HLogEdit(long transactionId, final byte [] row, BatchOperation op,
      long timestamp) {
    this(new KeyValue(row, op.getColumn(), timestamp,
      op.isPut()? KeyValue.Type.Put: KeyValue.Type.Delete, op.getValue()));
    // This covers delete ops too...
    this.transactionId = transactionId;
    this.operation = TransactionalOperation.WRITE;
    this.isTransactionEntry = true;
  }

  /** Construct a transactional operation (BEGIN, ABORT, or COMMIT). 
   * 
   * @param transactionId
   * @param op
   */
  public HLogEdit(long transactionId, TransactionalOperation op) {
    this.kv = KeyValue.LOWESTKEY;
    this.transactionId = transactionId;
    this.operation = op;
    this.isTransactionEntry = true;
  }

  /** @return the KeyValue */
  public KeyValue getKeyValue() {
    return this.kv;
  }

  /** @return true if entry is a transactional entry */
  public boolean isTransactionEntry() {
    return isTransactionEntry;
  }
  
  /**
   * Get the transactionId, or null if this is not a transactional edit.
   * 
   * @return Return the transactionId.
   */
  public Long getTransactionId() {
    return transactionId;
  }

  /**
   * Get the operation.
   * 
   * @return Return the operation.
   */
  public TransactionalOperation getOperation() {
    return operation;
  }

  /**
   * @return First column name, timestamp, and first 128 bytes of the value
   * bytes as a String.
   */
  @Override
  public String toString() {
    String value = "";
    try {
      value = (this.kv.getValueLength() > MAX_VALUE_LEN)?
        new String(this.kv.getValue(), 0, MAX_VALUE_LEN,
          HConstants.UTF8_ENCODING) + "...":
        new String(this.kv.getValue(), HConstants.UTF8_ENCODING);
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException("UTF8 encoding not present?", e);
    }
    return this.kv.toString() +
      (isTransactionEntry ? "/tran=" + transactionId + "/op=" +
        operation.toString(): "") + "/value=" + value;
  }
  
  // Writable

  public void write(DataOutput out) throws IOException {
    Bytes.writeByteArray(out, kv.getBuffer(), kv.getOffset(), kv.getLength());
    out.writeBoolean(isTransactionEntry);
    if (isTransactionEntry) {
      out.writeLong(transactionId);
      out.writeUTF(operation.name());
    }
  }
  
  public void readFields(DataInput in) throws IOException {
    byte [] kvbytes = Bytes.readByteArray(in);
    this.kv = new KeyValue(kvbytes, 0, kvbytes.length);
    isTransactionEntry = in.readBoolean();
    if (isTransactionEntry) {
      transactionId = in.readLong();
      operation = TransactionalOperation.valueOf(in.readUTF());
    }
  }

  /**
   * @param value
   * @return True if an entry and its content is {@link #DELETED_BYTES}.
   */
  public static boolean isDeleted(final byte [] value) {
    return isDeleted(value, 0, value.length);
  }

  /**
   * @param value
   * @return True if an entry and its content is {@link #DELETED_BYTES}.
   */
  public static boolean isDeleted(final byte [] value, final int offset,
      final int length) {
    return (value == null)? false:
      Bytes.BYTES_RAWCOMPARATOR.compare(DELETED_BYTES, 0, DELETED_BYTES.length,
        value, offset, length) == 0;
  }
}