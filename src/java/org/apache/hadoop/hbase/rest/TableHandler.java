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
import java.net.URLDecoder;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
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
      else if (pathSegments[1].toLowerCase().equals(ROW)) {
        // get a row
        getRow(table, request, response, pathSegments);
      }
      else{
        doNotFound(response, "Not handled in TableHandler");
      }
    }
  }
  
  public void doPost(HttpServletRequest request, HttpServletResponse response, 
    String[] pathSegments)
  throws ServletException, IOException {
    putRow(request, response, pathSegments);
  }
  
  public void doPut(HttpServletRequest request, HttpServletResponse response, 
    String[] pathSegments)
  throws ServletException, IOException {
    doPost(request, response, pathSegments);
  }
  
  public void doDelete(HttpServletRequest request, HttpServletResponse response, 
    String[] pathSegments)
  throws ServletException, IOException {
    deleteRow(request, response, pathSegments);
  }
  
  /*
   * @param request
   * @param response
   * @param pathSegments info path split on the '/' character.  First segment
   * is the tablename, second is 'row', and third is the row id.
   * @throws IOException
   * Retrieve a row in one of several output formats.
   */
  private void getRow(HTable table, final HttpServletRequest request,
    final HttpServletResponse response, final String [] pathSegments)
  throws IOException {
    // pull the row key out of the path
    String row = URLDecoder.decode(pathSegments[2], HConstants.UTF8_ENCODING);

    String timestampStr = null;
    if (pathSegments.length == 4) {
      // A timestamp has been supplied.
      timestampStr = pathSegments[3];
      if (timestampStr.equals("timestamps")) {
        // Not supported in hbase just yet. TODO
        doMethodNotAllowed(response, "Not yet supported by hbase");
        return;
      }
    }
    
    String[] columns = request.getParameterValues(COLUMN);
        
    if (columns == null || columns.length == 0) {
      // They want full row returned. 

      // Presumption is that this.table has already been focused on target table.
      Map<byte [], Cell> result = timestampStr == null ? 
        table.getRow(Bytes.toBytes(row)) 
        : table.getRow(Bytes.toBytes(row), Long.parseLong(timestampStr));
        
      if (result == null || result.size() == 0) {
        doNotFound(response, "Row not found!");
      } else {
        switch (ContentType.getContentType(request.getHeader(ACCEPT))) {
        case XML:
          outputRowXml(response, result);
          break;
        case MIME:
        default:
          doNotAcceptable(response, "Unsupported Accept Header Content: " +
            request.getHeader(CONTENT_TYPE));
        }
      }
    } else {
      Map<byte [], Cell> prefiltered_result = table.getRow(Bytes.toBytes(row));
    
      if (prefiltered_result == null || prefiltered_result.size() == 0) {
        doNotFound(response, "Row not found!");
      } else {
        // create a Set from the columns requested so we can
        // efficiently filter the actual found columns
        Set<String> requested_columns_set = new HashSet<String>();
        for(int i = 0; i < columns.length; i++){
          requested_columns_set.add(columns[i]);
        }
  
        // output map that will contain the filtered results
        Map<byte [], Cell> m =
          new TreeMap<byte [], Cell>(Bytes.BYTES_COMPARATOR);

        // get an array of all the columns retrieved
        Set<byte []> columns_retrieved = prefiltered_result.keySet();

        // copy over those cells with requested column names
        for(byte [] current_column: columns_retrieved) {
          if (requested_columns_set.contains(Bytes.toString(current_column))) {
            m.put(current_column, prefiltered_result.get(current_column));            
          }
        }
        
        switch (ContentType.getContentType(request.getHeader(ACCEPT))) {
          case XML:
            outputRowXml(response, m);
            break;
          case MIME:
          default:
            doNotAcceptable(response, "Unsupported Accept Header Content: " +
              request.getHeader(CONTENT_TYPE));
        }
      }
    }
  }
  
  /*
   * Output a row encoded as XML.
   * @param response
   * @param result
   * @throws IOException
   */
  private void outputRowXml(final HttpServletResponse response,
      final Map<byte [], Cell> result)
  throws IOException {
    setResponseHeader(response, result.size() > 0? 200: 204,
        ContentType.XML.toString());
    XMLOutputter outputter = getXMLOutputter(response.getWriter());
    outputter.startTag(ROW);
    outputColumnsXml(outputter, result);
    outputter.endTag();
    outputter.endDocument();
    outputter.getWriter().close();
  }

  /*
   * @param response
   * @param result
   * Output the results contained in result as a multipart/related response.
   */
  // private void outputRowMime(final HttpServletResponse response,
  //     final Map<Text, Cell> result)
  // throws IOException {
  //   response.setStatus(result.size() > 0? 200: 204);
  //   // This code ties me to the jetty server.
  //   MultiPartResponse mpr = new MultiPartResponse(response);
  //   // Content type should look like this for multipart:
  //   // Content-type: multipart/related;start="<rootpart*94ebf1e6-7eb5-43f1-85f4-2615fc40c5d6@example.jaxws.sun.com>";type="application/xop+xml";boundary="uuid:94ebf1e6-7eb5-43f1-85f4-2615fc40c5d6";start-info="text/xml"
  //   String ct = ContentType.MIME.toString() + ";charset=\"UTF-8\";boundary=\"" +
  //     mpr.getBoundary() + "\"";
  //   // Setting content type is broken.  I'm unable to set parameters on the
  //   // content-type; They get stripped.  Can't set boundary, etc.
  //   // response.addHeader("Content-Type", ct);
  //   response.setContentType(ct);
  //   outputColumnsMime(mpr, result);
  //   mpr.close();
  // }
  
  /*
   * @param request
   * @param response
   * @param pathSegments
   * Do a put based on the client request.
   */
  private void putRow(final HttpServletRequest request,
    final HttpServletResponse response, final String [] pathSegments)
  throws IOException, ServletException {
    HTable table = getTable(pathSegments[0]);

    // pull the row key out of the path
    String row = URLDecoder.decode(pathSegments[2], HConstants.UTF8_ENCODING);
    
    switch(ContentType.getContentType(request.getHeader(CONTENT_TYPE))) {
      case XML:
        putRowXml(table, row, request, response, pathSegments);
        break;
      case MIME:
        doNotAcceptable(response, "Don't support multipart/related yet...");
        break;
      default:
        doNotAcceptable(response, "Unsupported Accept Header Content: " +
          request.getHeader(CONTENT_TYPE));
    }
  }

  /*
   * @param request
   * @param response
   * @param pathSegments
   * Decode supplied XML and do a put to Hbase.
   */
  private void putRowXml(HTable table, String row, 
    final HttpServletRequest request, final HttpServletResponse response, 
    final String [] pathSegments)
  throws IOException, ServletException{

    DocumentBuilderFactory docBuilderFactory 
      = DocumentBuilderFactory.newInstance();  
    //ignore all comments inside the xml file
    docBuilderFactory.setIgnoringComments(true);

    DocumentBuilder builder = null;
    Document doc = null;
    
    String timestamp = pathSegments.length >= 4 ? pathSegments[3] : null;
    
    try{
      builder = docBuilderFactory.newDocumentBuilder();
      doc = builder.parse(request.getInputStream());
    } catch (javax.xml.parsers.ParserConfigurationException e) {
      throw new ServletException(e);
    } catch (org.xml.sax.SAXException e){
      throw new ServletException(e);
    }

    BatchUpdate batchUpdate;
    
    try{
      // start an update
      batchUpdate = timestamp == null ? 
        new BatchUpdate(row) : new BatchUpdate(row, Long.parseLong(timestamp));

      // set the columns from the xml
      NodeList columns = doc.getElementsByTagName("column");

      for(int i = 0; i < columns.getLength(); i++){
        // get the current column element we're working on
        Element column = (Element)columns.item(i);

        // extract the name and value children
        Node name_node = column.getElementsByTagName("name").item(0);
        String name = name_node.getFirstChild().getNodeValue();

        Node value_node = column.getElementsByTagName("value").item(0);

        byte[] value = new byte[0];
        
        // for some reason there's no value here. probably indicates that
        // the consumer passed a null as the cell value.
        if(value_node.getFirstChild() != null && 
          value_node.getFirstChild().getNodeValue() != null){
          // decode the base64'd value
          value = org.apache.hadoop.hbase.util.Base64.decode(
            value_node.getFirstChild().getNodeValue());
        }

        // put the value
        batchUpdate.put(name, value);
      }

      // commit the update
      table.commit(batchUpdate);
      
      // respond with a 200
      response.setStatus(200);      
    }
    catch(Exception e){
      throw new ServletException(e);
    }
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
  
  /*
   * @param request
   * @param response
   * @param pathSegments
   * Delete some or all cells for a row.
   */
   private void deleteRow(final HttpServletRequest request,
    final HttpServletResponse response, final String [] pathSegments)
  throws IOException, ServletException {
    // grab the table we're operating on
    HTable table = getTable(getTableName(pathSegments));
    
    // pull the row key out of the path
    String row = URLDecoder.decode(pathSegments[2], HConstants.UTF8_ENCODING);

    String[] columns = request.getParameterValues(COLUMN);
        
    // hack - we'll actually test for the presence of the timestamp parameter
    // eventually
    boolean timestamp_present = false;
    if(timestamp_present){ // do a timestamp-aware delete
      doMethodNotAllowed(response, "DELETE with a timestamp not implemented!");
    }
    else{ // ignore timestamps
      if(columns == null || columns.length == 0){
        // retrieve all the columns
        doMethodNotAllowed(response,
          "DELETE without specified columns not implemented!");
      } else{
        // delete each column in turn      
        for(int i = 0; i < columns.length; i++){
          table.deleteAll(row, columns[i]);
        }
      }
      response.setStatus(202);
    }
  } 
}
