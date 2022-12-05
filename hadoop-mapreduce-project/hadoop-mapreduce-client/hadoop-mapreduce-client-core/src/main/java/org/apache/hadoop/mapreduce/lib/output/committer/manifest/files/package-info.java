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
 * Persistence formats.
 * These are the persistence formats used for passing data from tasks
 * to the job committer
 * {@link org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.TaskManifest},
 * and for a {@code _SUCCESS} file, which is in
 * {@link org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.ManifestSuccessData}.
 * The {@code _SUCCESS} file is a copy of the S3A Committer
 * {@code org.apache.hadoop.fs.s3a.commit.files.ManifestSuccessData},
 * the intent being that at the JSON-level they are compatible.
 * This is to aid testing/validation and support calls, with one single
 * format to load.
 *
 * Consult the individual formats for their declarations of access;
 * the _SUCCESS file is one which tests may use.
 *
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
package org.apache.hadoop.mapreduce.lib.output.committer.manifest.files;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;