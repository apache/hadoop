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

package org.apache.hadoop.mapreduce.v2.app.webapp.jsonprovider;

import javax.ws.rs.core.Feature;
import javax.ws.rs.core.FeatureContext;

import org.glassfish.jersey.CommonProperties;
import org.glassfish.jersey.internal.InternalProperties;

public class JsonProviderFeature implements Feature {
  @Override
  public boolean configure(FeatureContext context) {
    //Auto discovery should be disabled to ensure the custom providers will be used
    context.property(CommonProperties.MOXY_JSON_FEATURE_DISABLE, true);
    context.property(InternalProperties.JSON_FEATURE, "JsonProviderFeature");
    context.register(IncludeRootJSONProvider.class, 2001);
    context.register(ExcludeRootJSONProvider.class, 2002);
    return true;
  }
}
