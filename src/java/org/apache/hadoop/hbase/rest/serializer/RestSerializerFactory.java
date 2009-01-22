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
package org.apache.hadoop.hbase.rest.serializer;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.hbase.rest.Dispatcher.ContentType;
import org.apache.hadoop.hbase.rest.exception.HBaseRestException;

/**
 * 
 *         Factory used to return a Rest Serializer tailored to the HTTP
 *         Requesters accept type in the header.
 * 
 */
public class RestSerializerFactory {

  public static AbstractRestSerializer getSerializer(
      HttpServletRequest request, HttpServletResponse response)
      throws HBaseRestException {
    ContentType ct = ContentType.getContentType(request.getHeader("accept"));
    AbstractRestSerializer serializer = null;

    // TODO refactor this so it uses reflection to create the new objects.
    switch (ct) {
    case XML:
      serializer = new SimpleXMLSerializer(response);
      break;
    case JSON:
      serializer = new JSONSerializer(response);
      break;
    default:
      serializer = new SimpleXMLSerializer(response);
      break;
    }
    return serializer;
  }
}
