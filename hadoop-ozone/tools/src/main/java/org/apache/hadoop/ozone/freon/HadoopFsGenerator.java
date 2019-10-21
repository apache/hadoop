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
import java.util.concurrent.Callable;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;

import com.codahale.metrics.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Data generator tool test om performance.
 */
@Command(name = "dfsg",
    aliases = "dfs-file-generator",
    description = "Create random files to the any dfs compatible file system.",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true,
    showDefaultValues = true)
public class HadoopFsGenerator extends BaseFreonGenerator
    implements Callable<Void> {

  private static final Logger LOG =
      LoggerFactory.getLogger(HadoopFsGenerator.class);

  @Option(names = {"--path"},
      description = "Hadoop FS file system path",
      defaultValue = "o3fs://bucket1.vol1")
  private String rootPath;

  @Option(names = {"-s", "--size"},
      description = "Size of the generated files (in bytes)",
      defaultValue = "10240")
  private int fileSize;

  @Option(names = {"--buffer"},
      description = "Size of buffer used to generated the key content.",
      defaultValue = "4096")
  private int bufferSize;

  private ContentGenerator contentGenerator;

  private Timer timer;

  private FileSystem fileSystem;

  @Override
  public Void call() throws Exception {

    init();

    OzoneConfiguration configuration = createOzoneConfiguration();

    fileSystem = FileSystem.get(URI.create(rootPath), configuration);

    contentGenerator = new ContentGenerator(fileSize, bufferSize);

    timer = getMetrics().timer("file-create");

    runTests(this::createFile);

    return null;
  }

  private void createFile(long counter) throws Exception {
    Path file = new Path(rootPath + "/" + generateObjectName(counter));
    fileSystem.mkdirs(file.getParent());

    timer.time(() -> {
      try (FSDataOutputStream output = fileSystem.create(file)) {
        contentGenerator.write(output);
      }
      return null;
    });
  }
}
