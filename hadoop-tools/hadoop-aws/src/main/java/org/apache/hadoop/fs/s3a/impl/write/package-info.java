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
 * Classes related to writing objects.
 * <p>
 * {@link org.apache.hadoop.fs.s3a.impl.write.StoreWriter} and
 * its implementation {@link org.apache.hadoop.fs.s3a.impl.write.StoreWriterService}
 * export the object model of S3 itself.
 * <p>
 * The {@code Write*} classes bridge from the filesystem APIs to the store, being
 * invoked by filesystem, output stream and committer services.
 */
@InterfaceAudience.Private
package org.apache.hadoop.fs.s3a.impl.write;

import org.apache.hadoop.classification.InterfaceAudience;