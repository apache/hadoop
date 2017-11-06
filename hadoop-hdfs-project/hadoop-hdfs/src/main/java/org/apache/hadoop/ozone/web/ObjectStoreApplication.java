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

package org.apache.hadoop.ozone.web;

import org.apache.hadoop.ozone.client.rest.OzoneExceptionMapper;
import org.apache.hadoop.ozone.web.handlers.BucketHandler;
import org.apache.hadoop.ozone.web.handlers.KeyHandler;
import org.apache.hadoop.ozone.web.handlers.ServiceFilter;
import org.apache.hadoop.ozone.web.handlers.VolumeHandler;
import org.apache.hadoop.ozone.web.messages.LengthInputStreamMessageBodyWriter;
import org.apache.hadoop.ozone.web.messages.StringMessageBodyWriter;

import javax.ws.rs.core.Application;
import java.util.HashSet;
import java.util.Set;

/**
 * Ozone Application.
 */
public class ObjectStoreApplication extends Application {
  public ObjectStoreApplication() {
    super();
  }

  @Override
  public Set<Class<?>> getClasses() {
    HashSet<Class<?>> set = new HashSet<>();
    set.add(BucketHandler.class);
    set.add(VolumeHandler.class);
    set.add(KeyHandler.class);
    set.add(OzoneExceptionMapper.class);
    set.add(LengthInputStreamMessageBodyWriter.class);
    set.add(StringMessageBodyWriter.class);
    return set;
  }

  @Override
  public Set<Object> getSingletons() {
    HashSet<Object> set = new HashSet<>();
    set.add(ServiceFilter.class);
    return set;
  }
}
