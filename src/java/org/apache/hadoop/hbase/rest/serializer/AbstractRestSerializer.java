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

import javax.servlet.http.HttpServletResponse;

/**
 * 
 *         Abstract object that is used as the base of all serializers in the
 *         REST based interface.
 */
public abstract class AbstractRestSerializer implements IRestSerializer {

  // keep the response object to write back to the stream
  protected final HttpServletResponse response;
  // Used to denote if pretty printing of the output should be used
  protected final boolean prettyPrint;

  /**
   * marking the default constructor as private so it will never be used.
   */
  @SuppressWarnings("unused")
  private AbstractRestSerializer() {
    response = null;
    prettyPrint = false;
  }

  /**
   * Public constructor for AbstractRestSerializer. This is the constructor that
   * should be called whenever creating a RestSerializer object.
   * 
   * @param response
   */
  public AbstractRestSerializer(HttpServletResponse response,
      boolean prettyPrint) {
    super();
    this.response = response;
    this.prettyPrint = prettyPrint;
  }

}
