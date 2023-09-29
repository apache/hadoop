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

package org.apache.hadoop.fs.azurebfs.commit;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitter;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterFactory;

import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.OPT_STORE_OPERATIONS_CLASS;

/**
 * A Committer for the manifest committer which performs all bindings needed
 * to work best with abfs.
 * This includes, at a minimum, switching to the abfs-specific manifest store operations.
 *
 * This classname is referenced in configurations, so MUST NOT change.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class AzureManifestCommitterFactory extends ManifestCommitterFactory {

  /**
   * Classname, which can be declared in job configurations.
   */
  public static final String NAME = ManifestCommitterFactory.class.getName();

  @Override
  public ManifestCommitter createOutputCommitter(final Path outputPath,
      final TaskAttemptContext context) throws IOException {
    final Configuration conf = context.getConfiguration();
    // use ABFS Store operations
    conf.set(OPT_STORE_OPERATIONS_CLASS,
        AbfsManifestStoreOperations.NAME);
    return super.createOutputCommitter(outputPath, context);
  }
}
