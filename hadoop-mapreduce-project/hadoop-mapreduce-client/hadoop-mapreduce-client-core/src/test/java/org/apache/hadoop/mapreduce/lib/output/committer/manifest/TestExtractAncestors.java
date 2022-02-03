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

package org.apache.hadoop.mapreduce.lib.output.committer.manifest;

import java.util.Arrays;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.test.HadoopTestBase;

import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.CreateOutputDirectoriesStage.extractAncestors;

/**
 * Test the {@code CreateOutputDirectoriesStage.extractAncestors()} operation
 * to verify that it builds a set of ancestors paths of the inputs.
 * This is used when building the list of parent directories to
 * probe for being files if the job is set to prepare parent directories.
 */
public class TestExtractAncestors extends HadoopTestBase {

  private static final Path BASE = new Path("file:///base");
  private static final Path DEST = new Path(BASE, "dest");
  private static final Path DEST_1 = new Path(DEST, "1");
  private static final Path DEST_2 = new Path(DEST, "2");
  private static final Path DEST_1_1 = new Path(DEST_1, "1");
  private static final Path DEST_1_2 = new Path(DEST_1, "2");
  private static final Path DEST_1_1_1 = new Path(DEST_1_1, "1");
  private static final Path DEST_1_1_2 = new Path(DEST_1_1, "2");
  private static final Path DEST_2_1 = new Path(DEST_2, "1");
  private static final Path DEST_2_2 = new Path(DEST_2, "2");

  @Test
  public void testEmptySet() {
    Assertions.assertThat(extract())
        .describedAs("empty ancestor set")
        .isEmpty();
  }

  @Test
  public void testRootEmpty() {
    Assertions.assertThat(extract(new Path("/")))
        .describedAs("extracted root")
        .isEmpty();
  }

  @Test
  public void testDestDir() {
    Assertions.assertThat(extract(DEST))
        .describedAs("dest dir should have no ancestors")
        .isEmpty();
  }

  @Test
  public void testParallelEntry() {
    final Path parallel = new Path(BASE, "parallel");
    Assertions.assertThat(extract(parallel))
        .describedAs("paralle path to dest dir should ")
        .isNotEmpty();
  }

  @Test
  public void testExtract1Parent() {
    Assertions.assertThat(extract(DEST_1_1, DEST_1_2))
        .describedAs("Source collection with two dirs under same parent")
        .containsExactlyInAnyOrder(DEST_1);
  }

  @Test
  public void testExtract2Parent() {
    Assertions.assertThat(extract(DEST_1_1, DEST_1_2, DEST_2_1, DEST_2_2))
        .describedAs("Source collection with two subtreees")
        .containsExactlyInAnyOrder(DEST_1, DEST_2);
  }

  @Test
  public void testExtractDuplicates() {
    Assertions.assertThat(extract(DEST_1_1, DEST_1_2, DEST_2_1, DEST_2_2, DEST_1_1, DEST_1_2))
        .describedAs("Source collection with duplicates")
        .containsExactlyInAnyOrder(DEST_1, DEST_2);
  }

  @Test
  public void testExtractParentsInSet() {
    Assertions.assertThat(extract(DEST_1, DEST_2, DEST_1_1, DEST_1_2, DEST_2_1, DEST_2_2))
        .describedAs("extraction with ancestors in directoriesToCreate")
        .isEmpty();
  }

  @Test
  public void testExtractDeep() {
    Assertions.assertThat(extract(DEST_1_1_1, DEST_1_1_2))
        .describedAs("deep extraction with multiple ancestors")
        .containsExactlyInAnyOrder(DEST_1, DEST_1_1);
  }

  private Set<Path> extract(Path... paths) {
    return extractAncestors(DEST, Arrays.asList(paths));
  }

}
