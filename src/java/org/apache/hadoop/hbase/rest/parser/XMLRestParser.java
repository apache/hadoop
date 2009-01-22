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
package org.apache.hadoop.hbase.rest.parser;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HColumnDescriptor.CompressionType;
import org.apache.hadoop.hbase.rest.RESTConstants;
import org.apache.hadoop.hbase.rest.descriptors.RowUpdateDescriptor;
import org.apache.hadoop.hbase.rest.descriptors.ScannerDescriptor;
import org.apache.hadoop.hbase.rest.exception.HBaseRestException;
import org.apache.hadoop.hbase.thrift.generated.Hbase;
import org.apache.hadoop.hbase.util.Bytes;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * 
 */
public class XMLRestParser implements IHBaseRestParser {

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.hbase.rest.parser.IHBaseRestParser#getTableDescriptor
   * (byte[])
   */
  public HTableDescriptor getTableDescriptor(byte[] input)
      throws HBaseRestException {
    DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory
        .newInstance();
    docBuilderFactory.setIgnoringComments(true);

    DocumentBuilder builder = null;
    Document doc = null;
    HTableDescriptor htd = null;

    try {
      builder = docBuilderFactory.newDocumentBuilder();
      ByteArrayInputStream is = new ByteArrayInputStream(input);
      doc = builder.parse(is);
    } catch (Exception e) {
      throw new HBaseRestException(e);
    }

    try {
      Node name_node = doc.getElementsByTagName("name").item(0);
      String table_name = name_node.getFirstChild().getNodeValue();

      htd = new HTableDescriptor(table_name);
      NodeList columnfamily_nodes = doc.getElementsByTagName("columnfamily");
      for (int i = 0; i < columnfamily_nodes.getLength(); i++) {
        Element columnfamily = (Element) columnfamily_nodes.item(i);
        htd.addFamily(this.getColumnDescriptor(columnfamily));
      }
    } catch (Exception e) {
      throw new HBaseRestException(e);
    }
    return htd;
  }

  public HColumnDescriptor getColumnDescriptor(Element columnfamily) {
    return this.getColumnDescriptor(columnfamily, null);
  }

  private HColumnDescriptor getColumnDescriptor(Element columnfamily,
      HTableDescriptor currentTDesp) {
    Node name_node = columnfamily.getElementsByTagName("name").item(0);
    String colname = makeColumnName(name_node.getFirstChild().getNodeValue());

    int max_versions = HColumnDescriptor.DEFAULT_VERSIONS;
    CompressionType compression = HColumnDescriptor.DEFAULT_COMPRESSION;
    boolean in_memory = HColumnDescriptor.DEFAULT_IN_MEMORY;
    boolean block_cache = HColumnDescriptor.DEFAULT_BLOCKCACHE;
    int max_cell_size = HColumnDescriptor.DEFAULT_LENGTH;
    int ttl = HColumnDescriptor.DEFAULT_TTL;
    boolean bloomfilter = HColumnDescriptor.DEFAULT_BLOOMFILTER;

    if (currentTDesp != null) {
      HColumnDescriptor currentCDesp = currentTDesp.getFamily(Bytes
          .toBytes(colname));
      if (currentCDesp != null) {
        max_versions = currentCDesp.getMaxVersions();
        // compression = currentCDesp.getCompression();
        in_memory = currentCDesp.isInMemory();
        block_cache = currentCDesp.isBlockCacheEnabled();
        max_cell_size = currentCDesp.getMaxValueLength();
        ttl = currentCDesp.getTimeToLive();
        bloomfilter = currentCDesp.isBloomfilter();
      }
    }

    NodeList max_versions_list = columnfamily
        .getElementsByTagName("max-versions");
    if (max_versions_list.getLength() > 0) {
      max_versions = Integer.parseInt(max_versions_list.item(0).getFirstChild()
          .getNodeValue());
    }

    NodeList compression_list = columnfamily
        .getElementsByTagName("compression");
    if (compression_list.getLength() > 0) {
      compression = CompressionType.valueOf(compression_list.item(0)
          .getFirstChild().getNodeValue());
    }

    NodeList in_memory_list = columnfamily.getElementsByTagName("in-memory");
    if (in_memory_list.getLength() > 0) {
      in_memory = Boolean.valueOf(in_memory_list.item(0).getFirstChild()
          .getNodeValue());
    }

    NodeList block_cache_list = columnfamily
        .getElementsByTagName("block-cache");
    if (block_cache_list.getLength() > 0) {
      block_cache = Boolean.valueOf(block_cache_list.item(0).getFirstChild()
          .getNodeValue());
    }

    NodeList max_cell_size_list = columnfamily
        .getElementsByTagName("max-cell-size");
    if (max_cell_size_list.getLength() > 0) {
      max_cell_size = Integer.valueOf(max_cell_size_list.item(0)
          .getFirstChild().getNodeValue());
    }

    NodeList ttl_list = columnfamily.getElementsByTagName("time-to-live");
    if (ttl_list.getLength() > 0) {
      ttl = Integer.valueOf(ttl_list.item(0).getFirstChild().getNodeValue());
    }

    NodeList bloomfilter_list = columnfamily
        .getElementsByTagName("bloomfilter");
    if (bloomfilter_list.getLength() > 0) {
      bloomfilter = Boolean.valueOf(bloomfilter_list.item(0).getFirstChild()
          .getNodeValue());
    }

    HColumnDescriptor hcd = new HColumnDescriptor(Bytes.toBytes(colname),
        max_versions, compression, in_memory, block_cache, max_cell_size, ttl,
        bloomfilter);

    NodeList metadataList = columnfamily.getElementsByTagName("metadata");
    for (int i = 0; i < metadataList.getLength(); i++) {
      Element metadataColumn = (Element) metadataList.item(i);
      // extract the name and value children
      Node mname_node = metadataColumn.getElementsByTagName("name").item(0);
      String mname = mname_node.getFirstChild().getNodeValue();
      Node mvalue_node = metadataColumn.getElementsByTagName("value").item(0);
      String mvalue = mvalue_node.getFirstChild().getNodeValue();
      hcd.setValue(mname, mvalue);
    }

    return hcd;
  }

  protected String makeColumnName(String column) {
    String returnColumn = column;
    if (column.indexOf(':') == -1)
      returnColumn += ':';
    return returnColumn;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.hbase.rest.parser.IHBaseRestParser#getColumnDescriptors
   * (byte[])
   */
  public ArrayList<HColumnDescriptor> getColumnDescriptors(byte[] input)
      throws HBaseRestException {
    DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory
        .newInstance();
    docBuilderFactory.setIgnoringComments(true);

    DocumentBuilder builder = null;
    Document doc = null;
    ArrayList<HColumnDescriptor> columns = new ArrayList<HColumnDescriptor>();

    try {
      builder = docBuilderFactory.newDocumentBuilder();
      ByteArrayInputStream is = new ByteArrayInputStream(input);
      doc = builder.parse(is);
    } catch (Exception e) {
      throw new HBaseRestException(e);
    }

    NodeList columnfamily_nodes = doc.getElementsByTagName("columnfamily");
    for (int i = 0; i < columnfamily_nodes.getLength(); i++) {
      Element columnfamily = (Element) columnfamily_nodes.item(i);
      columns.add(this.getColumnDescriptor(columnfamily));
    }

    return columns;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.hbase.rest.parser.IHBaseRestParser#getScannerDescriptor
   * (byte[])
   */
  public ScannerDescriptor getScannerDescriptor(byte[] input)
      throws HBaseRestException {
    // TODO Auto-generated method stub
    return null;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.hbase.rest.parser.IHBaseRestParser#getRowUpdateDescriptor
   * (byte[], byte[][])
   */
  public RowUpdateDescriptor getRowUpdateDescriptor(byte[] input,
      byte[][] pathSegments) throws HBaseRestException {
    RowUpdateDescriptor rud = new RowUpdateDescriptor();

    rud.setTableName(Bytes.toString(pathSegments[0]));
    rud.setRowName(Bytes.toString(pathSegments[2]));

    DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory
        .newInstance();
    docBuilderFactory.setIgnoringComments(true);

    DocumentBuilder builder = null;
    Document doc = null;

    try {
      builder = docBuilderFactory.newDocumentBuilder();
      ByteArrayInputStream is = new ByteArrayInputStream(input);
      doc = builder.parse(is);
    } catch (Exception e) {
      throw new HBaseRestException(e.getMessage(), e);
    }

    NodeList cell_nodes = doc.getElementsByTagName(RESTConstants.COLUMN);
    System.out.println("cell_nodes.length: " + cell_nodes.getLength());
    for (int i = 0; i < cell_nodes.getLength(); i++) {
      String columnName = null;
      byte[] value = null;

      Element cell = (Element) cell_nodes.item(i);

      NodeList item = cell.getElementsByTagName(RESTConstants.NAME);
      if (item.getLength() > 0) {
        columnName = item.item(0).getFirstChild().getNodeValue();
      }

      NodeList item1 = cell.getElementsByTagName(RESTConstants.VALUE);
      if (item1.getLength() > 0) {
        value = org.apache.hadoop.hbase.util.Base64.decode(item1
            .item(0).getFirstChild().getNodeValue());
      }

      if (columnName != null && value != null) {
        rud.getColVals().put(columnName.getBytes(), value);
      }
    }
    return rud;
  }
}
