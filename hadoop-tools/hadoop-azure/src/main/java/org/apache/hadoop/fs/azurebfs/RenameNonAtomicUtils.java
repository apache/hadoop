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

package org.apache.hadoop.fs.azurebfs;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.services.BlobProperty;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

public class RenameNonAtomicUtils extends RenameAtomicityUtils {

  RenameNonAtomicUtils(final AzureBlobFileSystem azureBlobFileSystem,
      final Path srcPath,
      final Path dstPath,
      final TracingContext tracingContext) throws IOException {
    super(azureBlobFileSystem, srcPath, dstPath, tracingContext);
  }

  @Override
  public void preRename(final List<BlobProperty> blobPropertyList)
      throws IOException {

  }

  @Override
  public void cleanup() throws IOException {

  }
}
