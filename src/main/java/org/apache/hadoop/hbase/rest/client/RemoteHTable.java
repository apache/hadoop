/*
 * Copyright 2010 The Apache Software Foundation
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

package org.apache.hadoop.hbase.rest.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.util.StringUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RowLock;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.rest.Constants;
import org.apache.hadoop.hbase.rest.model.CellModel;
import org.apache.hadoop.hbase.rest.model.CellSetModel;
import org.apache.hadoop.hbase.rest.model.RowModel;
import org.apache.hadoop.hbase.rest.model.ScannerModel;
import org.apache.hadoop.hbase.rest.model.TableSchemaModel;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * HTable interface to remote tables accessed via REST gateway
 */
public class RemoteHTable implements HTableInterface {

  private static final Log LOG = LogFactory.getLog(RemoteHTable.class);

  final Client client;
  final Configuration conf;
  final byte[] name;
  final String accessToken;
  final int maxRetries;
  final long sleepTime;

  @SuppressWarnings("unchecked")
  protected String buildRowSpec(final byte[] row, final Map familyMap, 
      final long startTime, final long endTime, final int maxVersions) {
    StringBuffer sb = new StringBuffer();
    sb.append('/');
    if (accessToken != null) {
      sb.append(accessToken);
      sb.append('/');
    }
    sb.append(Bytes.toStringBinary(name));
    sb.append('/');
    sb.append(Bytes.toStringBinary(row));
    Set families = familyMap.entrySet();
    if (families != null) {
      Iterator i = familyMap.entrySet().iterator();
      if (i.hasNext()) {
        sb.append('/');
      }
      while (i.hasNext()) {
        Map.Entry e = (Map.Entry)i.next();
        Collection quals = (Collection)e.getValue();
        if (quals != null && !quals.isEmpty()) {
          Iterator ii = quals.iterator();
          while (ii.hasNext()) {
            sb.append(Bytes.toStringBinary((byte[])e.getKey()));
            sb.append(':');
            Object o = ii.next();
            // Puts use byte[] but Deletes use KeyValue
            if (o instanceof byte[]) {
              sb.append(Bytes.toStringBinary((byte[])o));
            } else if (o instanceof KeyValue) {
              sb.append(Bytes.toStringBinary(((KeyValue)o).getQualifier()));
            } else {
              throw new RuntimeException("object type not handled");
            }
            if (ii.hasNext()) {
              sb.append(',');
            }
          }
        } else {
          sb.append(Bytes.toStringBinary((byte[])e.getKey()));
          sb.append(':');
        }
        if (i.hasNext()) {
          sb.append(',');
        }
      }
    }
    if (startTime != 0 && endTime != Long.MAX_VALUE) {
      sb.append('/');
      sb.append(startTime);
      if (startTime != endTime) {
        sb.append(',');
        sb.append(endTime);
      }
    } else if (endTime != Long.MAX_VALUE) {
      sb.append('/');
      sb.append(endTime);
    }
    if (maxVersions > 1) {
      sb.append("?v=");
      sb.append(maxVersions);
    }
    return sb.toString();
  }

  protected Result[] buildResultFromModel(final CellSetModel model) {
    List<Result> results = new ArrayList<Result>();
    for (RowModel row: model.getRows()) {
      List<KeyValue> kvs = new ArrayList<KeyValue>();
      for (CellModel cell: row.getCells()) {
        byte[][] split = KeyValue.parseColumn(cell.getColumn());
        byte[] column = split[0];
        byte[] qualifier = split.length > 1 ? split[1] : null;
        kvs.add(new KeyValue(row.getKey(), column, qualifier, 
          cell.getTimestamp(), cell.getValue()));
      }
      results.add(new Result(kvs));
    }
    return results.toArray(new Result[results.size()]);
  }

  protected CellSetModel buildModelFromPut(Put put) {
    RowModel row = new RowModel(put.getRow());
    long ts = put.getTimeStamp();
    for (List<KeyValue> kvs: put.getFamilyMap().values()) {
      for (KeyValue kv: kvs) {
        row.addCell(new CellModel(kv.getFamily(), kv.getQualifier(),
          ts != HConstants.LATEST_TIMESTAMP ? ts : kv.getTimestamp(),
          kv.getValue()));
      }
    }
    CellSetModel model = new CellSetModel();
    model.addRow(row);
    return model;
  }

  /**
   * Constructor
   * @param client
   * @param name
   */
  public RemoteHTable(Client client, String name) {
    this(client, HBaseConfiguration.create(), Bytes.toBytes(name), null);
  }

  /**
   * Constructor
   * @param client
   * @param name
   * @param accessToken
   */
  public RemoteHTable(Client client, String name, String accessToken) {
    this(client, HBaseConfiguration.create(), Bytes.toBytes(name), accessToken);
  }

  /**
   * Constructor
   * @param client
   * @param conf
   * @param name
   * @param accessToken
   */
  public RemoteHTable(Client client, Configuration conf, String name,
      String accessToken) {
    this(client, conf, Bytes.toBytes(name), accessToken);
  }

  /**
   * Constructor
   * @param conf
   */
  public RemoteHTable(Client client, Configuration conf, byte[] name,
      String accessToken) {
    this.client = client;
    this.conf = conf;
    this.name = name;
    this.accessToken = accessToken;
    this.maxRetries = conf.getInt("hbase.rest.client.max.retries", 10);
    this.sleepTime = conf.getLong("hbase.rest.client.sleep", 1000);
  }

  public byte[] getTableName() {
    return name.clone();
  }

  public Configuration getConfiguration() {
    return conf;
  }

  public HTableDescriptor getTableDescriptor() throws IOException {
    StringBuilder sb = new StringBuilder();
    sb.append('/');
    if (accessToken != null) {
      sb.append(accessToken);
      sb.append('/');
    }
    sb.append(Bytes.toStringBinary(name));
    sb.append('/');
    sb.append("schema");
    for (int i = 0; i < maxRetries; i++) {
      Response response = client.get(sb.toString(), Constants.MIMETYPE_PROTOBUF);
      int code = response.getCode();
      switch (code) {
      case 200:
        TableSchemaModel schema = new TableSchemaModel();
        schema.getObjectFromMessage(response.getBody());
        return schema.getTableDescriptor();
      case 509: 
        try {
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) { }
        break;
      default:
        throw new IOException("schema request returned " + code);
      }
    }
    throw new IOException("schema request timed out");
  }

  public void close() throws IOException {
    client.shutdown();
  }

  public Result get(Get get) throws IOException {
    TimeRange range = get.getTimeRange();
    String spec = buildRowSpec(get.getRow(), get.getFamilyMap(),
      range.getMin(), range.getMax(), get.getMaxVersions());
    if (get.getFilter() != null) {
      LOG.warn("filters not supported on gets");
    }
    for (int i = 0; i < maxRetries; i++) {
      Response response = client.get(spec, Constants.MIMETYPE_PROTOBUF);
      int code = response.getCode();
      switch (code) {
      case 200:
        CellSetModel model = new CellSetModel();
        model.getObjectFromMessage(response.getBody());
        Result[] results = buildResultFromModel(model);
        if (results.length > 0) {
          if (results.length > 1) {
            LOG.warn("too many results for get (" + results.length + ")");
          }
          return results[0];
        }
        // fall through
      case 404:
        return new Result();
      case 509:
        try {
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) { }
        break;
      default:
        throw new IOException("get request returned " + code);
      }
    }
    throw new IOException("get request timed out");
  }

  public boolean exists(Get get) throws IOException {
    LOG.warn("exists() is really get(), just use get()");
    Result result = get(get);
    return (result != null && !(result.isEmpty()));
  }

  public void put(Put put) throws IOException {
    CellSetModel model = buildModelFromPut(put);
    StringBuilder sb = new StringBuilder();
    sb.append('/');
    if (accessToken != null) {
      sb.append(accessToken);
      sb.append('/');      
    }
    sb.append(Bytes.toStringBinary(name));
    sb.append('/');
    sb.append(Bytes.toStringBinary(put.getRow()));
    for (int i = 0; i < maxRetries; i++) {
      Response response = client.put(sb.toString(), Constants.MIMETYPE_PROTOBUF,
        model.createProtobufOutput());
      int code = response.getCode();
      switch (code) {
      case 200:
        return;
      case 509:
        try {
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) { }
        break;
      default:
        throw new IOException("put request failed with " + code);
      }
    }
    throw new IOException("put request timed out");
  }

  public void put(List<Put> puts) throws IOException {
    // this is a trick: The gateway accepts multiple rows in a cell set and
    // ignores the row specification in the URI

    // separate puts by row
    TreeMap<byte[],List<KeyValue>> map =
      new TreeMap<byte[],List<KeyValue>>(Bytes.BYTES_COMPARATOR);
    for (Put put: puts) {
      byte[] row = put.getRow();
      List<KeyValue> kvs = map.get(row);
      if (kvs == null) {
        kvs = new ArrayList<KeyValue>();
        map.put(row, kvs);
      }
      for (List<KeyValue> l: put.getFamilyMap().values()) {
        kvs.addAll(l);
      }
    }

    // build the cell set
    CellSetModel model = new CellSetModel();
    for (Map.Entry<byte[], List<KeyValue>> e: map.entrySet()) {
      RowModel row = new RowModel(e.getKey());
      for (KeyValue kv: e.getValue()) {
        row.addCell(new CellModel(kv));
      }
      model.addRow(row);
    }

    // build path for multiput
    StringBuilder sb = new StringBuilder();
    sb.append('/');
    if (accessToken != null) {
      sb.append(accessToken);
      sb.append('/');      
    }
    sb.append(Bytes.toStringBinary(name));
    sb.append("/$multiput"); // can be any nonexistent row
    for (int i = 0; i < maxRetries; i++) {
      Response response = client.put(sb.toString(), Constants.MIMETYPE_PROTOBUF,
        model.createProtobufOutput());
      int code = response.getCode();
      switch (code) {
      case 200:
        return;
      case 509:
        try {
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) { }
        break;
      default:
        throw new IOException("multiput request failed with " + code);
      }
    }
    throw new IOException("multiput request timed out");
  }

  public void delete(Delete delete) throws IOException {
    String spec = buildRowSpec(delete.getRow(), delete.getFamilyMap(),
      delete.getTimeStamp(), delete.getTimeStamp(), 1);
    for (int i = 0; i < maxRetries; i++) {
      Response response = client.delete(spec);
      int code = response.getCode();
      switch (code) {
      case 200:
        return;
      case 509:
        try {
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) { }
        break;
      default:
        throw new IOException("delete request failed with " + code);
      }
    }
    throw new IOException("delete request timed out");
  }

  public void delete(List<Delete> deletes) throws IOException {
    for (Delete delete: deletes) {
      delete(delete);
    }
  }

  public void flushCommits() throws IOException {
    // no-op
  }

  class Scanner implements ResultScanner {

    String uri;

    public Scanner(Scan scan) throws IOException {
      ScannerModel model;
      try {
        model = ScannerModel.fromScan(scan);
      } catch (Exception e) {
        throw new IOException(e);
      }
      StringBuffer sb = new StringBuffer();
      sb.append('/');
      if (accessToken != null) {
        sb.append(accessToken);
        sb.append('/');
      }
      sb.append(Bytes.toStringBinary(name));
      sb.append('/');
      sb.append("scanner");
      for (int i = 0; i < maxRetries; i++) {
        Response response = client.post(sb.toString(),
          Constants.MIMETYPE_PROTOBUF, model.createProtobufOutput());
        int code = response.getCode();
        switch (code) {
        case 201:
          uri = response.getLocation();
          return;
        case 509:
          try {
            Thread.sleep(sleepTime);
          } catch (InterruptedException e) { }
          break;
        default:
          throw new IOException("scan request failed with " + code);
        }
      }
      throw new IOException("scan request timed out");
    }

    @Override
    public Result[] next(int nbRows) throws IOException {
      StringBuilder sb = new StringBuilder(uri);
      sb.append("?n=");
      sb.append(nbRows);
      for (int i = 0; i < maxRetries; i++) {
        Response response = client.get(sb.toString(),
          Constants.MIMETYPE_PROTOBUF);
        int code = response.getCode();
        switch (code) {
        case 200:
          CellSetModel model = new CellSetModel();
          model.getObjectFromMessage(response.getBody());
          return buildResultFromModel(model);
        case 204:
        case 206:
          return null;
        case 509:
          try {
            Thread.sleep(sleepTime);
          } catch (InterruptedException e) { }
          break;
        default:
          throw new IOException("scanner.next request failed with " + code);
        }
      }
      throw new IOException("scanner.next request timed out");
    }

    @Override
    public Result next() throws IOException {
      Result[] results = next(1);
      if (results == null || results.length < 1) {
        return null;
      }
      return results[0];
    }
    
    class Iter implements Iterator<Result> {

      Result cache;

      public Iter() {
        try {
          cache = Scanner.this.next();
        } catch (IOException e) {
          LOG.warn(StringUtils.stringifyException(e));
        }
      }

      @Override
      public boolean hasNext() {
        return cache != null;
      }

      @Override
      public Result next() {
        Result result = cache;
        try {
          cache = Scanner.this.next();
        } catch (IOException e) {
          LOG.warn(StringUtils.stringifyException(e));
          cache = null;
        }
        return result;
      }

      @Override
      public void remove() {
        throw new RuntimeException("remove() not supported");
      }
      
    }

    @Override
    public Iterator<Result> iterator() {
      return new Iter();
    }

    @Override
    public void close() {
      try {
        client.delete(uri);
      } catch (IOException e) {
        LOG.warn(StringUtils.stringifyException(e));
      }
    }

  }

  public ResultScanner getScanner(Scan scan) throws IOException {
    return new Scanner(scan);
  }

  public ResultScanner getScanner(byte[] family) throws IOException {
    Scan scan = new Scan();
    scan.addFamily(family);
    return new Scanner(scan);
  }

  public ResultScanner getScanner(byte[] family, byte[] qualifier)
      throws IOException {
    Scan scan = new Scan();
    scan.addColumn(family, qualifier);
    return new Scanner(scan);
  }

  public boolean isAutoFlush() {
    return true;
  }

  public Result getRowOrBefore(byte[] row, byte[] family) throws IOException {
    throw new IOException("getRowOrBefore not supported");
  }

  public RowLock lockRow(byte[] row) throws IOException {
    throw new IOException("lockRow not implemented");
  }

  public void unlockRow(RowLock rl) throws IOException {
    throw new IOException("unlockRow not implemented");
  }

  public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier,
      byte[] value, Put put) throws IOException {
    throw new IOException("checkAndPut not supported");
  }

  public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier,
      byte[] value, Delete delete) throws IOException {
    throw new IOException("checkAndDelete not supported");
  }


  public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier,
      long amount) throws IOException {
    throw new IOException("incrementColumnValue not supported");
  }

  public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier,
      long amount, boolean writeToWAL) throws IOException {
    throw new IOException("incrementColumnValue not supported");
  }

}
