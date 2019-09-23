/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.freon;

import java.net.URI;
import java.security.MessageDigest;
import java.util.concurrent.Callable;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;

import com.codahale.metrics.Timer;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Data generator tool test om performance.
 */
@Command(name = "dfsv",
    aliases = "dfs-file-validator",
    description = "Validate if the generated files have the same hash.",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true,
    showDefaultValues = true)
public class HadoopFsValidator extends BaseFreonGenerator
    implements Callable<Void> {

  private static final Logger LOG =
      LoggerFactory.getLogger(HadoopFsValidator.class);

  @Option(names = {"--path"},
      description = "Hadoop FS file system path",
      defaultValue = "o3fs://bucket1.vol1")
  private String rootPath;

  private ContentGenerator contentGenerator;

  private Timer timer;

  private FileSystem fileSystem;

  private byte[] referenceDigest;

  @Override
  public Void call() throws Exception {

    init();

    OzoneConfiguration configuration = createOzoneConfiguration();

    fileSystem = FileSystem.get(URI.create(rootPath), configuration);

    Path file = new Path(rootPath + "/" + generateObjectName(0));
    try (FSDataInputStream stream = fileSystem.open(file)) {
      referenceDigest = getDigest(stream);
    }

    timer = getMetrics().timer("file-read");

    runTests(this::validateFile);

    return null;
  }

  private void validateFile(long counter) throws Exception {
    Path file = new Path(rootPath + "/" + generateObjectName(counter));

    byte[] content = timer.time(() -> {
      try (FSDataInputStream input = fileSystem.open(file)) {
        return IOUtils.toByteArray(input);
      }
    });

    if (!MessageDigest.isEqual(referenceDigest, getDigest(content))) {
      throw new IllegalStateException(
          "Reference (=first) message digest doesn't match with digest of "
              + file.toString());
    }
  }
}
