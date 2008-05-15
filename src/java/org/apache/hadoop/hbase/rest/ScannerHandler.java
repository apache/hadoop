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
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scanner;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JenkinsHash;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.io.Text;
import org.mortbay.servlet.MultiPartResponse;
import org.znerd.xmlenc.XMLOutputter;

/**
 * ScannderHandler fields all scanner related requests. 
 */
public class ScannerHandler extends GenericHandler {

  public ScannerHandler(HBaseConfiguration conf, HBaseAdmin admin) 
  throws ServletException{
    super(conf, admin);
  }
    
  private class ScannerRecord {
    private final Scanner scanner;
    private RowResult next;
    
    ScannerRecord(final Scanner s) {
      this.scanner = s;
    }
  
    public Scanner getScanner() {
      return this.scanner;
    }
    
    public boolean hasNext() throws IOException {
      if (next == null) {
        next = scanner.next();
        return next != null;
      } else {
        return true;
      }
    }
    
    /**
     * Call next on the scanner.
     * @return Null if finished, RowResult otherwise
     * @throws IOException
     */
    public RowResult next() throws IOException {
      if (!hasNext()) {
        return null;
      }
      RowResult temp = next;
      next = null;
      return temp;
    }
  }
  
  /*
   * Map of outstanding scanners keyed by scannerid.
   */
  private final Map<String, ScannerRecord> scanners =
    new HashMap<String, ScannerRecord>();
  
  public void doGet(HttpServletRequest request, HttpServletResponse response, 
    String[] pathSegments)
  throws ServletException, IOException {
    doMethodNotAllowed(response, "GET to a scanner not supported.");
  }
  
  public void doPost(HttpServletRequest request, HttpServletResponse response, 
    String[] pathSegments)
  throws ServletException, IOException {
    if (pathSegments.length == 2) {
      // trying to create a scanner
      openScanner(request, response, pathSegments);
    }
    else if (pathSegments.length == 3) {
      // advancing a scanner
      getScanner(request, response, pathSegments[2]);
    }
    else{
      doNotFound(response, "No handler for request");
    }
  }
  
  public void doPut(HttpServletRequest request, HttpServletResponse response, 
    String[] pathSegments)
  throws ServletException, IOException {
    doPost(request, response, pathSegments);
  }
  
  public void doDelete(HttpServletRequest request, HttpServletResponse response, 
    String[] pathSegments)
  throws ServletException, IOException {
    deleteScanner(response, pathSegments[2]);
  }
  
  /*
   * Advance scanner and return current position.
   * @param request
   * @param response
   * @param scannerid
   * @throws IOException
   */
  private void getScanner(final HttpServletRequest request,
      final HttpServletResponse response, final String scannerid)
  throws IOException {
    ScannerRecord sr = this.scanners.get(scannerid);
    if (sr == null) {
      doNotFound(response, "No such scanner.");
      return;
    }

    if (sr.hasNext()) {
      switch (ContentType.getContentType(request.getHeader(ACCEPT))) {
        case XML:
          outputScannerEntryXML(response, sr);
          break;
        case MIME:
/*          outputScannerEntryMime(response, sr);*/
          doNotAcceptable(response);
          break;
        default:
          doNotAcceptable(response);
      }
    }
    else{
      this.scanners.remove(scannerid);
      doNotFound(response, "Scanner is expended");
    }
  }

  private void outputScannerEntryXML(final HttpServletResponse response,
    final ScannerRecord sr)
  throws IOException {
    RowResult rowResult = sr.next();
    
    // respond with a 200 and Content-type: text/xml
    setResponseHeader(response, 200, ContentType.XML.toString());
    
    // setup an xml outputter
    XMLOutputter outputter = getXMLOutputter(response.getWriter());
    
    outputter.startTag(ROW);
    
    // write the row key
    doElement(outputter, "name", 
      org.apache.hadoop.hbase.util.Base64.encodeBytes(rowResult.getRow()));
    
    outputColumnsXml(outputter, rowResult);
    outputter.endTag();
    outputter.endDocument();
    outputter.getWriter().close();
  }

  // private void outputScannerEntryMime(final HttpServletResponse response,
  //   final ScannerRecord sr)
  // throws IOException {
  //   response.setStatus(200);
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
  //   // Write row, key-column and timestamp each in its own part.
  //   mpr.startPart("application/octet-stream",
  //       new String [] {"Content-Description: row",
  //         "Content-Transfer-Encoding: binary",
  //         "Content-Length: " + sr.getKey().getRow().getBytes().length});
  //   mpr.getOut().write(sr.getKey().getRow().getBytes());
  // 
  //   // Usually key-column is empty when scanning.
  //   if (sr.getKey().getColumn() != null &&
  //       sr.getKey().getColumn().getLength() > 0) {
  //     mpr.startPart("application/octet-stream",
  //       new String [] {"Content-Description: key-column",
  //         "Content-Transfer-Encoding: binary",
  //         "Content-Length: " + sr.getKey().getColumn().getBytes().length});
  //   }
  //   mpr.getOut().write(sr.getKey().getColumn().getBytes());
  //   // TODO: Fix. Need to write out the timestamp in the ordained timestamp
  //   // format.
  //   byte [] timestampBytes = Long.toString(sr.getKey().getTimestamp()).getBytes();
  //   mpr.startPart("application/octet-stream",
  //       new String [] {"Content-Description: timestamp",
  //         "Content-Transfer-Encoding: binary",
  //         "Content-Length: " + timestampBytes.length});
  //   mpr.getOut().write(timestampBytes);
  //   // Write out columns
  //   outputColumnsMime(mpr, sr.getValue());
  //   mpr.close();
  // }
  
  /*
   * Create scanner
   * @param request
   * @param response
   * @param pathSegments
   * @throws IOException
   */
  private void openScanner(final HttpServletRequest request,
      final HttpServletResponse response, final String [] pathSegments)
  throws IOException, ServletException {
    // get the table
    HTable table = getTable(getTableName(pathSegments));
    
    // get the list of columns we're supposed to interact with
    String[] raw_columns = request.getParameterValues(COLUMN);
    byte [][] columns = null;
    
    if (raw_columns != null) {
      columns = new byte [raw_columns.length][];
      for (int i = 0; i < raw_columns.length; i++) {
        // I think this decoding is redundant.
        columns[i] =
          Bytes.toBytes(URLDecoder.decode(raw_columns[i], HConstants.UTF8_ENCODING));
      }
    } else {
      // TODO: Need to put into the scanner all of the table's column
      // families.  TODO: Verify this returns all rows.  For now just fail.
      doMethodNotAllowed(response, "Unspecified columns parameter currently not supported!");
      return;
    }

    // TODO: Parse according to the timestamp format we agree on.    
    String raw_ts = request.getParameter(TIMESTAMP);

    // TODO: Are these decodings redundant?
    byte [] startRow = request.getParameter(START_ROW) == null?
      HConstants.EMPTY_START_ROW:
      Bytes.toBytes(URLDecoder.decode(request.getParameter(START_ROW),
        HConstants.UTF8_ENCODING));
    // Empty start row is same value as empty end row.
    byte [] endRow = request.getParameter(END_ROW) == null?
      HConstants.EMPTY_START_ROW:
      Bytes.toBytes(URLDecoder.decode(request.getParameter(END_ROW),
        HConstants.UTF8_ENCODING));

    Scanner scanner = (request.getParameter(END_ROW) == null)?
       table.getScanner(columns, startRow):
       table.getScanner(columns, startRow, endRow);
    
    // Make a scanner id by hashing the object toString value (object name +
    // an id).  Will make identifier less burdensome and more url friendly.
    String scannerid =
      Integer.toHexString(JenkinsHash.hash(scanner.toString().getBytes(), -1));
    ScannerRecord sr = new ScannerRecord(scanner);
    
    // store the scanner for subsequent requests
    this.scanners.put(scannerid, sr);
    
    // set a 201 (Created) header and a Location pointing to the new
    // scanner
    response.setStatus(201);
    response.addHeader("Location", request.getContextPath() + "/" +
      pathSegments[0] + "/" + pathSegments[1] + "/" + scannerid);
    response.getOutputStream().close();
  }

  /*
   * Delete scanner
   * @param response
   * @param scannerid
   * @throws IOException
   */
  private void deleteScanner(final HttpServletResponse response,
      final String scannerid)
  throws IOException, ServletException {
    ScannerRecord sr = this.scanners.remove(scannerid);
    if (sr == null) {
      doNotFound(response, "No such scanner");
    } else {
      sr.getScanner().close();
      response.setStatus(200);
      response.getOutputStream().close();
    }
  }
}
