/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.webapp.jsonprovider;

import javax.ws.rs.core.Feature;
import javax.ws.rs.core.FeatureContext;

/**
 * A JAX-RS {@link Feature} that registers custom MOXy JSON providers
 * for handling serialization and deserialization of JSON with or without
 * root elements.
 *
 * <p>
 * This feature disables MOXy's automatic provider discovery to ensure
 * that the custom providers {@link IncludeRootJSONProvider} and
 * {@link ExcludeRootJSONProvider} are used explicitly with defined priorities.
 * </p>
 *
 * <p>Configuration details:</p>
 * <ul>
 *   <li>Registers {@link IncludeRootJSONProvider} with priority {@code 2001}.</li>
 *   <li>Registers {@link ExcludeRootJSONProvider} with priority {@code 2002}.</li>
 * </ul>
 *
 * @see IncludeRootJSONProvider
 * @see ExcludeRootJSONProvider
 * @see org.glassfish.jersey.CommonProperties#MOXY_JSON_FEATURE_DISABLE
 */
public class JsonProviderFeature implements Feature {

  /**
   * Do not use default constructor.
   */
  public JsonProviderFeature() {
  }

  /**
   * Configures the feature by registering the custom JSON providers.
   *
   * @param context the {@link FeatureContext} provided by the JAX-RS runtime
   * @return {@code true} to indicate that the feature was successfully configured
   */
  @Override
  public boolean configure(FeatureContext context) {
    // Priorities are used to maintain order between the JSONProviders.
    // This way, we can improve the determinism of the app.
    context.register(IncludeRootJSONProvider.class, 2001);
    context.register(ExcludeRootJSONProvider.class, 2002);
    return true;
  }
}
