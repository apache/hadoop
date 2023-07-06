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
 * Unless required by joblicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.webapp.resource;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.inject.Singleton;

import org.apache.hadoop.yarn.webapp.resource.MyTestWebService.MyInfo;

@Singleton
@Provider
public class MyTestObjectWriterContextResolver implements ContextResolver<ObjectWriter> {

  private ObjectWriter context;
  private final Set<Class> types;

  // you have to specify all the dao classes here
  private final Class[] cTypes = { MyInfo.class };

  public MyTestObjectWriterContextResolver() {
    this.types = new HashSet<>(Arrays.asList(cTypes));
    this.context = new ObjectMapper().writerFor(MyInfo.class);
  }

  @Override
  public ObjectWriter getContext(Class<?> objectType) {
    return (types.contains(objectType)) ? context : null;
  }
}