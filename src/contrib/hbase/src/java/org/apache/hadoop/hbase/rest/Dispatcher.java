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

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.hbase.HBaseAdmin;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * Servlet implementation class for hbase REST interface.
 * Presumes container ensures single thread through here at any one time
 * (Usually the default configuration).  In other words, code is not
 * written thread-safe.
 * <p>This servlet has explicit dependency on Jetty server; it uses the
 * jetty implementation of MultipartResponse.
 * 
 * <p>TODO:
 * <ul>
 * <li>multipart/related response is not correct; the servlet setContentType
 * is broken.  I am unable to add parameters such as boundary or start to
 * multipart/related.  They get stripped.</li>
 * <li>Currently creating a scanner, need to specify a column.  Need to make
 * it so the HTable instance has current table's metadata to-hand so easy to
 * find the list of all column families so can make up list of columns if none
 * specified.</li>
 * <li>Minor items are we are decoding URLs in places where probably already
 * done and how to timeout scanners that are in the scanner list.</li>
 * </ul>
 * @see <a href="http://wiki.apache.org/lucene-hadoop/Hbase/HbaseRest">Hbase REST Specification</a>
 */
public class Dispatcher extends javax.servlet.http.HttpServlet
implements javax.servlet.Servlet {
  
  private static final long serialVersionUID = 1045003206345359301L;
  
  private MetaHandler metaHandler;
  private TableHandler tableHandler;
  private ScannerHandler scannerHandler;

  private static final String SCANNER = "scanner";
  private static final String ROW = "row";
      
  /**
   * Default constructor
   */
  public Dispatcher() {
    super();
  }

  public void init() throws ServletException {
    super.init();
    
    HBaseConfiguration conf = new HBaseConfiguration();
    HBaseAdmin admin = null;
    
    try{
      admin = new HBaseAdmin(conf);
      metaHandler = new MetaHandler(conf, admin);
      tableHandler = new TableHandler(conf, admin);
      scannerHandler = new ScannerHandler(conf, admin);
    } catch(Exception e){
      throw new ServletException(e);
    }
  }

  protected void doGet(HttpServletRequest request, HttpServletResponse response)
  throws IOException, ServletException {
    String [] pathSegments = getPathSegments(request);
    
    if (pathSegments.length == 0 || pathSegments[0].length() <= 0) {
      // if it was a root request, then get some metadata about 
      // the entire instance.
      metaHandler.doGet(request, response, pathSegments);
    } else {
      // otherwise, it must be a GET request suitable for the
      // table handler.
      tableHandler.doGet(request, response, pathSegments);
    }
  }

  protected void doPost(HttpServletRequest request, HttpServletResponse response)
  throws IOException, ServletException {
    String [] pathSegments = getPathSegments(request);
    
    // there should be at least two path segments (table name and row or scanner)
    if (pathSegments.length >= 2 && pathSegments[0].length() > 0) {
      if (pathSegments[1].toLowerCase().equals(SCANNER) &&
          pathSegments.length >= 2) {
        scannerHandler.doPost(request, response, pathSegments);
        return;
      } else if (pathSegments[1].toLowerCase().equals(ROW) && pathSegments.length >= 3) {
        tableHandler.doPost(request, response, pathSegments);
        return;
      }
    }

    // if we get to this point, then no handler was matched this request.
    GenericHandler.doNotFound(response, "No handler for " + request.getPathInfo());
  }
  

  protected void doPut(HttpServletRequest request, HttpServletResponse response)
  throws ServletException, IOException {
    // Equate PUT with a POST.
    doPost(request, response);
  }

  protected void doDelete(HttpServletRequest request,
      HttpServletResponse response)
  throws IOException, ServletException {
    String [] pathSegments = getPathSegments(request);
    
    // must be at least two path segments (table name and row or scanner)
    if (pathSegments.length >= 2 && pathSegments[0].length() > 0) {
      // DELETE to a scanner requires at least three path segments
      if (pathSegments[1].toLowerCase().equals(SCANNER) &&
          pathSegments.length == 3 && pathSegments[2].length() > 0) {
        scannerHandler.doDelete(request, response, pathSegments);
        return;
      } else if (pathSegments[1].toLowerCase().equals(ROW) &&
          pathSegments.length >= 3) {
        tableHandler.doDelete(request, response, pathSegments);
        return;
      } 
    }
    
    // if we reach this point, then no handler exists for this request.
    GenericHandler.doNotFound(response, "No handler");
  }
  
  /*
   * @param request
   * @return request pathinfo split on the '/' ignoring the first '/' so first
   * element in pathSegment is not the empty string.
   */
  private String [] getPathSegments(final HttpServletRequest request) {
    int context_len = request.getContextPath().length() + 1;
    return request.getRequestURI().substring(context_len).split("/");
  }
}
