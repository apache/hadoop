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

package org.apache.hadoop.hbase.rest;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.ws.rs.Encoded;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.rest.transform.NullTransform;
import org.apache.hadoop.hbase.rest.transform.Transform;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StringUtils;

public class TableResource extends ResourceBase {
  private static final Log LOG = LogFactory.getLog(TableResource.class);

  /**
   * HCD attributes starting with this string are considered transform
   * directives
   */
  private static final String DIRECTIVE_KEY = "Transform$";

  /**
   * Transform directives are of the form <tt>&lt;qualifier&gt;:&lt;class&gt;</tt>
   * where <tt>qualifier</tt> is a string for exact matching or '*' as a wildcard
   * that will match anything; and <tt>class</tt> is either the fully qualified
   * class name of a transform implementation or can be the short name of a
   * transform in the <tt>org.apache.hadoop.hbase.rest.transform package</tt>.
   */
  private static final Pattern DIRECTIVE_PATTERN =
    Pattern.compile("([^\\:]+)\\:([^\\,]+)\\,?");
  private static final Transform defaultTransform = new NullTransform();
  private static final
    Map<String,Map<byte[],Map<byte[],Transform>>> transformMap =
      new ConcurrentHashMap<String,Map<byte[],Map<byte[],Transform>>>();
  private static final Map<String,Long> lastCheckedMap =
    new ConcurrentHashMap<String,Long>();

  /**
   * @param table the table
   * @param family the column family
   * @param qualifier the column qualifier, or null
   * @return the transformation specified for the given family or qualifier, if
   * any, otherwise the default
   */
  static Transform getTransform(String table, byte[] family, byte[] qualifier) {
    if (qualifier == null) {
      qualifier = HConstants.EMPTY_BYTE_ARRAY;
    }
    Map<byte[],Map<byte[],Transform>> familyMap = transformMap.get(table);
    if (familyMap != null) {
      Map<byte[],Transform> columnMap = familyMap.get(family);
      if (columnMap != null) {
        Transform t = columnMap.get(qualifier);
        // check as necessary if there is a wildcard entry
        if (t == null) {
          t = columnMap.get(HConstants.EMPTY_BYTE_ARRAY);
        }
        // if we found something, return it, otherwise we will return the
        // default by falling through
        if (t != null) {
          return t;
        }
      }
    }
    return defaultTransform;
  }

  synchronized static void setTransform(String table, byte[] family,
      byte[] qualifier, Transform transform) {
    Map<byte[],Map<byte[],Transform>> familyMap = transformMap.get(table);
    if (familyMap == null) {
      familyMap =  new ConcurrentSkipListMap<byte[],Map<byte[],Transform>>(
          Bytes.BYTES_COMPARATOR);
      transformMap.put(table, familyMap);
    }
    Map<byte[],Transform> columnMap = familyMap.get(family);
    if (columnMap == null) {
      columnMap = new ConcurrentSkipListMap<byte[],Transform>(
          Bytes.BYTES_COMPARATOR);
      familyMap.put(family, columnMap);
    }
    // if transform is null, remove any existing entry
    if (transform != null) {
      columnMap.put(qualifier, transform);
    } else {
      columnMap.remove(qualifier);
    }
  }

  String table;

  /**
   * Scan the table schema for transform directives. These are column family
   * attributes containing a comma-separated list of elements of the form
   * <tt>&lt;qualifier&gt;:&lt;transform-class&gt;</tt>, where qualifier
   * can be a string for exact matching or '*' as a wildcard to match anything.
   * The attribute key must begin with the string "Transform$".
   */
  void scanTransformAttrs() throws IOException {
    try {
      HBaseAdmin admin = new HBaseAdmin(servlet.getConfiguration());
      HTableDescriptor htd = admin.getTableDescriptor(Bytes.toBytes(table));
      for (HColumnDescriptor hcd: htd.getFamilies()) {
        for (Map.Entry<ImmutableBytesWritable, ImmutableBytesWritable> e:
            hcd.getValues().entrySet()) {
          // does the key start with the transform directive tag?
          String key = Bytes.toString(e.getKey().get());
          if (!key.startsWith(DIRECTIVE_KEY)) {
            // no, skip
            continue;
          }
          // match a comma separated list of one or more directives
          byte[] value = e.getValue().get();
          Matcher m = DIRECTIVE_PATTERN.matcher(Bytes.toString(value));
          while (m.find()) {
            byte[] qualifier = HConstants.EMPTY_BYTE_ARRAY;
            String s = m.group(1);
            if (s.length() > 0 && !s.equals("*")) {
              qualifier = Bytes.toBytes(s);
            }
            boolean retry = false;
            String className = m.group(2);
            while (true) {
              try {
                // if a transform was previously configured for the qualifier,
                // this will simply replace it
                setTransform(table, hcd.getName(), qualifier,
                  (Transform)Class.forName(className).newInstance());
                break;
              } catch (InstantiationException ex) {
                LOG.error(StringUtils.stringifyException(ex));
                if (retry) {
                  break;
                }
                retry = true;
              } catch (IllegalAccessException ex) {
                LOG.error(StringUtils.stringifyException(ex));
                if (retry) {
                  break;
                }
                retry = true;
              } catch (ClassNotFoundException ex) {
                if (retry) {
                  LOG.error(StringUtils.stringifyException(ex));
                  break;
                }
                className = "org.apache.hadoop.hbase.rest.transform." + className;
                retry = true;
              }
            }
          }
        }
      }
    } catch (TableNotFoundException e) {
      // ignore
    }
  }

  /**
   * Constructor
   * @param table
   * @throws IOException
   */
  public TableResource(String table) throws IOException {
    super();
    this.table = table;
    // Scanning the table schema is too expensive to do for every operation.
    // Do it once per minute by default.
    // Setting hbase.rest.transform.check.interval to <= 0 disables rescanning.
    long now = System.currentTimeMillis();
    Long lastChecked = lastCheckedMap.get(table);
    if (lastChecked != null) {
      long interval = servlet.getConfiguration()
        .getLong("hbase.rest.transform.check.interval", 60000);
      if (interval > 0 && (now - lastChecked.longValue()) > interval) {
        scanTransformAttrs();
        lastCheckedMap.put(table, now);
      }
    } else {
      scanTransformAttrs();
      lastCheckedMap.put(table, now);
    }
  }

  /** @return the table name */
  String getName() {
    return table;
  }

  /**
   * @return true if the table exists
   * @throws IOException
   */
  boolean exists() throws IOException {
    HBaseAdmin admin = new HBaseAdmin(servlet.getConfiguration());
    return admin.tableExists(table);
  }

  /**
   * Apply any configured transformations to the value
   * @param family
   * @param qualifier
   * @param value
   * @param direction
   * @return
   * @throws IOException
   */
  byte[] transform(byte[] family, byte[] qualifier, byte[] value,
      Transform.Direction direction) throws IOException {
    Transform t = getTransform(table, family, qualifier);
    if (t != null) {
      return t.transform(value, direction);
    }
    return value;
  }

  @Path("exists")
  public ExistsResource getExistsResource() throws IOException {
    return new ExistsResource(this);
  }

  @Path("regions")
  public RegionsResource getRegionsResource() throws IOException {
    return new RegionsResource(this);
  }

  @Path("scanner")
  public ScannerResource getScannerResource() throws IOException {
    return new ScannerResource(this);
  }

  @Path("schema")
  public SchemaResource getSchemaResource() throws IOException {
    return new SchemaResource(this);
  }

  @Path("multiget")
  public MultiRowResource getMultipleRowResource(
          final @QueryParam("v") String versions) throws IOException {
    return new MultiRowResource(this, versions);
  }

  @Path("{rowspec: .+}")
  public RowResource getRowResource(
      // We need the @Encoded decorator so Jersey won't urldecode before
      // the RowSpec constructor has a chance to parse
      final @PathParam("rowspec") @Encoded String rowspec,
      final @QueryParam("v") String versions) throws IOException {
    return new RowResource(this, rowspec, versions);
  }
}
