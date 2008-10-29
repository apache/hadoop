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
package org.apache.hadoop.hbase.rest;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HColumnDescriptor.CompressionType;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.znerd.xmlenc.XMLOutputter;


/**
 * TableHandler fields all requests that deal with an individual table.
 * That means all requests that start with /api/[table_name]/... go to 
 * this handler.
 */
public class TableHandler extends GenericHandler {
  public static final String DISABLE = "disable";
  public static final String ENABLE = "enable";
  
  public TableHandler(HBaseConfiguration conf, HBaseAdmin admin) 
  throws ServletException{
    super(conf, admin);
  }
  
  public void doGet(HttpServletRequest request, HttpServletResponse response, 
    String[] pathSegments)
  throws ServletException, IOException {
    // if it's just table name, return the metadata
    if (pathSegments.length == 1) {
      getTableMetadata(request, response, pathSegments[0]);
    }
    else{
      HTable table = getTable(pathSegments[0]);
      if (pathSegments[1].toLowerCase().equals(REGIONS)) {
        // get a region list
        getTableRegions(table, request, response);
      }
      else{
        doNotFound(response, "Not handled in TableHandler");
      }
    }
  }
  
  public void doPost(HttpServletRequest request, HttpServletResponse response, 
    String[] pathSegments)
  throws ServletException, IOException {
    if (pathSegments.length == 0 || pathSegments[0].length() <= 0) {
      // if it's a creation operation
      putTable(request, response, pathSegments);
    } else {
      // if it's a disable operation or enable operation
      String tableName = pathSegments[0];      
      if (pathSegments[1].toLowerCase().equals(DISABLE)) {
        admin.disableTable(tableName);
      } else if (pathSegments[1].toLowerCase().equals(ENABLE)) {
        admin.enableTable(tableName);
      }
      response.setStatus(202);
    }
  }

  public void doPut(HttpServletRequest request, HttpServletResponse response, 
    String[] pathSegments)
  throws ServletException, IOException {
    updateTable(request, response, pathSegments);
  }
  
  public void doDelete(HttpServletRequest request, HttpServletResponse response, 
    String[] pathSegments)
  throws ServletException, IOException {
    deleteTable(request, response, pathSegments);
  }

  /*
   * Return region offsets.
   * @param request
   * @param response
   */
  private void getTableRegions(HTable table, final HttpServletRequest request,
    final HttpServletResponse response)
  throws IOException {
    // Presumption is that this.table has already been focused on target table.
    byte [][] startKeys = table.getStartKeys();
    // Presumption is that this.table has already been set against target table
    switch (ContentType.getContentType(request.getHeader(ACCEPT))) {
      case XML:
        setResponseHeader(response, startKeys.length > 0? 200: 204,
            ContentType.XML.toString());
          XMLOutputter outputter = getXMLOutputter(response.getWriter());
          outputter.startTag("regions");
          for (int i = 0; i < startKeys.length; i++) {
            doElement(outputter, "region", Bytes.toString(startKeys[i]));
          }
          outputter.endTag();
          outputter.endDocument();
          outputter.getWriter().close();
        break;
      case PLAIN:
        setResponseHeader(response, startKeys.length > 0? 200: 204,
            ContentType.PLAIN.toString());
          PrintWriter out = response.getWriter();
          for (int i = 0; i < startKeys.length; i++) {
            // TODO: Add in the server location.  Is it needed?
            out.print(Bytes.toString(startKeys[i]));
          }
          out.close();
        break;
      case MIME:
      default:
        doNotAcceptable(response, "Unsupported Accept Header Content: " +
          request.getHeader(CONTENT_TYPE));
    }
  }
  /*
   * Get table metadata.
   * @param request
   * @param response
   * @param tableName
   * @throws IOException
   */
  private void getTableMetadata(final HttpServletRequest request,
      final HttpServletResponse response, final String tableName)
  throws IOException {
    HTableDescriptor [] tables = this.admin.listTables();
    HTableDescriptor descriptor = null;
    for (int i = 0; i < tables.length; i++) {
      if (Bytes.toString(tables[i].getName()).equals(tableName)) {
        descriptor = tables[i];
        break;
      }
    }
    if (descriptor == null) {
      doNotFound(response, "Table not found!");
    } else {
      // Presumption is that this.table has already been set against target table
      ContentType type = ContentType.getContentType(request.getHeader(ACCEPT));
      switch (type) {
      case XML:
        setResponseHeader(response, 200, ContentType.XML.toString());
        XMLOutputter outputter = getXMLOutputter(response.getWriter());
        outputter.startTag("table");
        doElement(outputter, "name", Bytes.toString(descriptor.getName()));
        outputter.startTag("columnfamilies");
        for (HColumnDescriptor e: descriptor.getFamilies()) {
          outputter.startTag("columnfamily");
          doElement(outputter, "name", Bytes.toString(e.getName()));
          doElement(outputter, "compression", e.getCompression().toString());
          doElement(outputter, "bloomfilter",
              Boolean.toString(e.isBloomfilter()));
          doElement(outputter, "max-versions",
            Integer.toString(e.getMaxVersions()));
          doElement(outputter, "maximum-cell-size",
              Integer.toString(e.getMaxValueLength()));
          outputter.endTag();
        }
        outputter.endTag();
        outputter.endTag();
        outputter.endDocument();
        outputter.getWriter().close();
        break;
      case PLAIN:
        setResponseHeader(response, 200, ContentType.PLAIN.toString());
        PrintWriter out = response.getWriter();
        out.print(descriptor.toString());
        out.close();
        break;
      case MIME:
      default:
        doNotAcceptable(response, "Unsupported Accept Header Content: " +
          request.getHeader(CONTENT_TYPE));
      }
    }
  }
  
  private void putTable(HttpServletRequest request,
    HttpServletResponse response, String[] pathSegments) 
  throws IOException, ServletException {
    switch(ContentType.getContentType(request.getHeader(CONTENT_TYPE))) {
      case XML:
        putTableXml(request, response, pathSegments);
        break;
      case MIME:
        doNotAcceptable(response, "Don't support multipart/related yet...");
        break;
      default:
        doNotAcceptable(response, "Unsupported Accept Header Content: " +
            request.getHeader(CONTENT_TYPE));
    }
  } 
  
  private void updateTable(HttpServletRequest request,
    HttpServletResponse response, String[] pathSegments) 
  throws IOException, ServletException {
    switch(ContentType.getContentType(request.getHeader(CONTENT_TYPE))) {
      case XML:
        updateTableXml(request, response, pathSegments);
        break;
      case MIME:
        doNotAcceptable(response, "Don't support multipart/related yet...");
        break;
      default:
        doNotAcceptable(response, "Unsupported Accept Header Content: " +
            request.getHeader(CONTENT_TYPE));
    }
  }
  
  private void deleteTable(HttpServletRequest request,
      HttpServletResponse response, String[] pathSegments)
  throws ServletException {
    try {
      String tableName = pathSegments[0];
      String[] column_params = request.getParameterValues(COLUMN);
      if (column_params != null && column_params.length > 0) {
        for (String column : column_params) {
          admin.deleteColumn(tableName, makeColumnName(column));
        }
      } else {
        admin.deleteTable(tableName);
      }
      response.setStatus(202);
    } catch (Exception e) {
      throw new ServletException(e);
    }
  }  
  
  private void putTableXml(HttpServletRequest 
    request, HttpServletResponse response, String[] pathSegments)
  throws IOException, ServletException {
    DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory
        .newInstance();
    // ignore all comments inside the xml file
    docBuilderFactory.setIgnoringComments(true);

    DocumentBuilder builder = null;
    Document doc = null;

    try {
      builder = docBuilderFactory.newDocumentBuilder();
      doc = builder.parse(request.getInputStream());
    } catch (javax.xml.parsers.ParserConfigurationException e) {
      throw new ServletException(e);
    } catch (org.xml.sax.SAXException e) {
      throw new ServletException(e);
    }
    
    try {
      Node name_node = doc.getElementsByTagName("name").item(0);
      String table_name = name_node.getFirstChild().getNodeValue();
      
      HTableDescriptor htd = new HTableDescriptor(table_name);
      NodeList columnfamily_nodes = doc.getElementsByTagName("columnfamily");
      for (int i = 0; i < columnfamily_nodes.getLength(); i++) {
        Element columnfamily = (Element)columnfamily_nodes.item(i);
        htd.addFamily(putColumnFamilyXml(columnfamily));
      }
      admin.createTable(htd);      
    } catch (Exception e) {
      throw new ServletException(e);
    }
  }

  private void updateTableXml(HttpServletRequest request,
      HttpServletResponse response, String[] pathSegments) throws IOException,
      ServletException {
    DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory
        .newInstance();
    // ignore all comments inside the xml file
    docBuilderFactory.setIgnoringComments(true);

    DocumentBuilder builder = null;
    Document doc = null;

    try {
      builder = docBuilderFactory.newDocumentBuilder();
      doc = builder.parse(request.getInputStream());
    } catch (javax.xml.parsers.ParserConfigurationException e) {
      throw new ServletException(e);
    } catch (org.xml.sax.SAXException e) {
      throw new ServletException(e);
    }

    try {
      String tableName = pathSegments[0];
      HTableDescriptor htd = admin.getTableDescriptor(tableName);
      
      NodeList columnfamily_nodes = doc.getElementsByTagName("columnfamily");

      for (int i = 0; i < columnfamily_nodes.getLength(); i++) {
        Element columnfamily = (Element) columnfamily_nodes.item(i);
        HColumnDescriptor hcd = putColumnFamilyXml(columnfamily, htd);
        if (htd.hasFamily(Bytes.toBytes(hcd.getNameAsString()))) {
          admin.modifyColumn(tableName, hcd.getNameAsString(), hcd);
        } else {
          admin.addColumn(tableName, hcd);
        }
      }
    } catch (Exception e) {
      throw new ServletException(e);
    }
  }
  
  private HColumnDescriptor putColumnFamilyXml(Element columnfamily) {
    return putColumnFamilyXml(columnfamily, null);
  }
  
  private HColumnDescriptor putColumnFamilyXml(Element columnfamily, HTableDescriptor currentTDesp) {
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
      HColumnDescriptor currentCDesp = currentTDesp.getFamily(Bytes.toBytes(colname));
      if (currentCDesp != null) {
        max_versions = currentCDesp.getMaxVersions();
        compression = currentCDesp.getCompression();
        in_memory = currentCDesp.isInMemory();
        block_cache = currentCDesp.isBlockCacheEnabled();
        max_cell_size = currentCDesp.getMaxValueLength();
        ttl = currentCDesp.getTimeToLive();
        bloomfilter = currentCDesp.isBloomfilter();
      }
    }
    
    NodeList max_versions_list = columnfamily.getElementsByTagName("max-versions");
    if (max_versions_list.getLength() > 0) {
      max_versions = Integer.parseInt(max_versions_list.item(0).getFirstChild().getNodeValue());
    }

    NodeList compression_list = columnfamily.getElementsByTagName("compression");
    if (compression_list.getLength() > 0) {
      compression = CompressionType.valueOf(compression_list.item(0).getFirstChild().getNodeValue());
    }

    NodeList in_memory_list = columnfamily.getElementsByTagName("in-memory");
    if (in_memory_list.getLength() > 0) {
      in_memory = Boolean.valueOf(in_memory_list.item(0).getFirstChild().getNodeValue());
    }
    
    NodeList block_cache_list = columnfamily.getElementsByTagName("block-cache");
    if (block_cache_list.getLength() > 0) {
      block_cache = Boolean.valueOf(block_cache_list.item(0).getFirstChild().getNodeValue());
    }

    NodeList max_cell_size_list = columnfamily.getElementsByTagName("max-cell-size");
    if (max_cell_size_list.getLength() > 0) {
      max_cell_size = Integer.valueOf(max_cell_size_list.item(0).getFirstChild().getNodeValue());
    }

    NodeList ttl_list = columnfamily.getElementsByTagName("time-to-live");
    if (ttl_list.getLength() > 0) {
      ttl = Integer.valueOf(ttl_list.item(0).getFirstChild().getNodeValue());
    }

    NodeList bloomfilter_list = columnfamily.getElementsByTagName("bloomfilter");
    if (bloomfilter_list.getLength() > 0) {
      bloomfilter = Boolean.valueOf(bloomfilter_list.item(0).getFirstChild().getNodeValue());
    }
    
    HColumnDescriptor hcd = new HColumnDescriptor(Bytes.toBytes(colname), max_versions,
        compression, in_memory, block_cache, max_cell_size, ttl, bloomfilter);
    
    NodeList metadataList = columnfamily.getElementsByTagName("metadata");
    for (int i = 0; i < metadataList.getLength(); i++) {
      Element metadataColumn = (Element)metadataList.item(i);
      // extract the name and value children
      Node mname_node = metadataColumn.getElementsByTagName("name").item(0);
      String mname = mname_node.getFirstChild().getNodeValue();
      Node mvalue_node = metadataColumn.getElementsByTagName("value").item(0);
      String mvalue = mvalue_node.getFirstChild().getNodeValue();
      hcd.setValue(mname, mvalue);
    }
    
    return hcd;
  }
}
