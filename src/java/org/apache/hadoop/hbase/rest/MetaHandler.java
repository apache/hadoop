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

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.znerd.xmlenc.XMLOutputter;


/**
 * MetaHandler fields all requests for metadata at the instance level. At the
 * momment this is only GET requests to /.
 */
public class MetaHandler extends GenericHandler {

  public MetaHandler(HBaseConfiguration conf, HBaseAdmin admin) 
  throws ServletException{
    super(conf, admin);
  }
   
   
  public void doGet(HttpServletRequest request, HttpServletResponse response, 
    String[] pathSegments)
  throws ServletException, IOException {
    getTables(request, response);
  }
  
  public void doPost(HttpServletRequest request, HttpServletResponse response, 
    String[] pathSegments)
  throws ServletException, IOException {
    doMethodNotAllowed(response, "POST not allowed at /");
  }
  
  public void doPut(HttpServletRequest request, HttpServletResponse response, 
    String[] pathSegments)
  throws ServletException, IOException {
    doMethodNotAllowed(response, "PUT not allowed at /");    
  }
  
  public void doDelete(HttpServletRequest request, HttpServletResponse response, 
    String[] pathSegments)
  throws ServletException, IOException {
    doMethodNotAllowed(response, "DELETE not allowed at /");
  }
     
  /*
   * Return list of tables. 
   * @param request
   * @param response
   */
  private void getTables(final HttpServletRequest request,
    final HttpServletResponse response)
  throws IOException {
    HTableDescriptor [] tables = this.admin.listTables();

    switch (ContentType.getContentType(request.getHeader(ACCEPT))) {
      case XML:
        setResponseHeader(response, tables.length > 0? 200: 204,
            ContentType.XML.toString());
          XMLOutputter outputter = getXMLOutputter(response.getWriter());
          outputter.startTag("tables");
          for (int i = 0; i < tables.length; i++) {
            doElement(outputter, "table", Bytes.toString(tables[i].getName()));
          }
          outputter.endTag();
          outputter.endDocument();
          outputter.getWriter().close();
        break;
      case PLAIN:
        setResponseHeader(response, tables.length > 0? 200: 204,
            ContentType.PLAIN.toString());
          PrintWriter out = response.getWriter();
          for (int i = 0; i < tables.length; i++) {
            out.println(Bytes.toString(tables[i].getName()));
          }
          out.close();
        break;
      default:
        doNotAcceptable(response);
    }
  }
}
