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
package org.apache.hadoop.test;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * Annotation for {@link TestJettyHelper} subclasses to indicate that the test method
 * requires a Jetty servlet-container.
 * <p/>
 * The {@link TestJettyHelper#getJettyServer()} returns a ready to configure Jetty
 * servlet-container. After registering contexts, servlets, filters the the Jetty
 * server must be started (<code>getJettyServer.start()</code>. The Jetty server
 * is automatically stopped at the end of the test method invocation.
 * <p/>
 * Use the {@link TestJettyHelper#getJettyURL()} to obtain the base URL
 * (schema://host:port) of the Jetty server.
 * <p/>
 * Refer to the {@link HTestCase} class for more details.
 */
@Retention(java.lang.annotation.RetentionPolicy.RUNTIME)
@Target(java.lang.annotation.ElementType.METHOD)
public @interface TestJetty {
}
