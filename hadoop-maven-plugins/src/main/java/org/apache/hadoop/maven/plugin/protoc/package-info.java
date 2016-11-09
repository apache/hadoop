/*
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

/**
 * <p>Plugins to generate Java files based on protocol buffers with the protoc
 * command.
 *
 * <p>For generated files intended for primary build artifacts use like:
 * <pre>
 *  &lt;plugins&gt;
 *    ... SNIP ...
 *    &lt;plugin&gt;
 *      &lt;groupId&gt;org.apache.hadoop&lt;/groupId&gt;
 *      &lt;artifactId&gt;hadoop-maven-plugins&lt;/artifactId&gt;
 *      &lt;executions&gt;
 *        ... SNIP ...
 *        &lt;execution&gt;
 *          &lt;id&gt;compile-protoc&lt;/id&gt;
 *          &lt;goals&gt;
 *            &lt;goal&gt;protoc&lt;/goal&gt;
 *          &lt;/goals&gt;
 *          &lt;configuration&gt;
 *            &lt;protocVersion&gt;${protobuf.version}&lt;/protocVersion&gt;
 *            &lt;protocCommand&gt;${protoc.path}&lt;/protocCommand&gt;
 *            &lt;imports&gt;
 *              &lt;param&gt;${basedir}/src/main/proto&lt;/param&gt;
 *            &lt;/imports&gt;
 *            &lt;source&gt;
 *              &lt;directory&gt;${basedir}/src/main/proto&lt;/directory&gt;
 *              &lt;includes&gt;
 *                &lt;include&gt;HAServiceProtocol.proto&lt;/include&gt;
 *                ... SNIP ...
 *                &lt;include&gt;RefreshCallQueueProtocol.proto&lt;/include&gt;
 *                &lt;include&gt;GenericRefreshProtocol.proto&lt;/include&gt;
 *              &lt;/includes&gt;
 *            &lt;/source&gt;
 *          &lt;/configuration&gt;
 *        &lt;/execution&gt;
 *        ... SNIP ...
 *      &lt;/executions&gt;
 *      ... SNIP ...
 *    &lt;/plugin&gt;
 *  &lt;/plugins&gt;
 * </pre>
 *
 * For generated files intended only for test, use like:
 * <pre>
 *  &lt;plugins&gt;
 *    ... SNIP ...
 *    &lt;plugin&gt;
 *      &lt;groupId&gt;org.apache.hadoop&lt;/groupId&gt;
 *      &lt;artifactId&gt;hadoop-maven-plugins&lt;/artifactId&gt;
 *      &lt;executions&gt;
 *        ... SNIP ...
 *        &lt;execution&gt;
 *          &lt;id&gt;compile-test-protoc&lt;/id&gt;
 *          &lt;goals&gt;
 *            &lt;goal&gt;test-protoc&lt;/goal&gt;
 *          &lt;/goals&gt;
 *          &lt;configuration&gt;
 *            &lt;protocVersion&gt;${protobuf.version}&lt;/protocVersion&gt;
 *            &lt;protocCommand&gt;${protoc.path}&lt;/protocCommand&gt;
 *            &lt;imports&gt;
 *              &lt;param&gt;${basedir}/src/test/proto&lt;/param&gt;
 *            &lt;/imports&gt;
 *            &lt;source&gt;
 *              &lt;directory&gt;${basedir}/src/test/proto&lt;/directory&gt;
 *              &lt;includes&gt;
 *                &lt;include&gt;test.proto&lt;/include&gt;
 *                &lt;include&gt;test_rpc_service.proto&lt;/include&gt;
 *              &lt;/includes&gt;
 *            &lt;/source&gt;
 *          &lt;/configuration&gt;
 *        &lt;/execution&gt;
 *        ... SNIP ...
 *      &lt;/executions&gt;
 *      ... SNIP ...
 *    &lt;/plugin&gt;
 *  &lt;/plugins&gt;
 * </pre>
 *
 */
package org.apache.hadoop.maven.plugin.protoc;
