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

package org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.FileEntry;
import org.apache.hadoop.test.AbstractHadoopTestBase;
import org.apache.hadoop.util.functional.RemoteIterators;

/**
 * Test {@link EntryFileIO}.
 */
public class TestEntryFileIO extends AbstractHadoopTestBase {

  private EntryFileIO entryFileIO;

  private File entryFile;

  @Before
  public void setup() throws Exception {
    entryFileIO = new EntryFileIO(new Configuration());
  }
  /**
   * Teardown.
   * @throws Exception on any failure
   */
  @After
  public void teardown() throws Exception {
    Thread.currentThread().setName("teardown");
    if (entryFile != null) {
      entryFile.delete();
    }
  }


  @Test
  public void testCreateWriteReadFile() throws Throwable {
    entryFile = File.createTempFile("entry", ".seq");
    final FileEntry source = new FileEntry("source", "dest", 100, "etag");
    SequenceFile.Writer writer = entryFileIO.createWriter(entryFile);
    writer.append(NullWritable.get(), source);
    writer.flush();
    writer.close();
    Assertions.assertThat(entryFile.length())
        .describedAs("Length of file %s", entryFile)
        .isGreaterThan(0);

    FileEntry readBack = new FileEntry();
    try (SequenceFile.Reader reader = entryFileIO.createReader(entryFile)) {
      reader.next(NullWritable.get(), readBack);
    }
    Assertions.assertThat(readBack)
        .describedAs("entry read back from sequence file")
        .isEqualTo(source);

    final RemoteIterator<FileEntry> it =
        entryFileIO.iterateOver(entryFileIO.createReader(entryFile));
    List<FileEntry> entries = new ArrayList<>();
    RemoteIterators.foreach(it, entries::add);
    Assertions.assertThat(entries)
        .hasSize(1)
        .element(0)
        .isEqualTo(source);

  }

}
