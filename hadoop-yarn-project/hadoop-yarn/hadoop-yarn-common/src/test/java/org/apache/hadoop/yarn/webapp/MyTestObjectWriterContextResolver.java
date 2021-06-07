/**
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
package org.apache.hadoop.yarn.webapp;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import javax.ws.rs.ext.ContextResolver;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class MyTestObjectWriterContextResolver implements ContextResolver<ObjectWriter> {

  private final ObjectWriter writer;
  private final Set<Class> types;
  private final Class[] cTypes = { MyTestWebService.MyInfo.class };

  public MyTestObjectWriterContextResolver() throws Exception {
    this.types = new HashSet<>(Arrays.asList(cTypes));
    this.writer = new ObjectMapper().writerFor(MyTestWebService.MyInfo.class);
  }

  @Override
  public ObjectWriter getContext(Class<?> type) {
    return (types.contains(type)) ? writer : null;
  }
}
