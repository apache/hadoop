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

package org.apache.hadoop.fs.s3a.s3guard;

import java.net.URI;

import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.test.AbstractHadoopTestBase;

import static org.apache.hadoop.fs.s3a.Constants.S3GUARD_METASTORE_DYNAMO;
import static org.apache.hadoop.fs.s3a.Constants.S3GUARD_METASTORE_LOCAL;
import static org.apache.hadoop.fs.s3a.Constants.S3_METADATA_STORE_IMPL;
import static org.apache.hadoop.fs.s3a.s3guard.S3Guard.NULL_METADATA_STORE;
import static org.apache.hadoop.fs.s3a.s3guard.S3Guard.checkNoS3Guard;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Verify thqt the metastore checking
 * is forgiving for local/null stores, and unforgiving for
 * DDB or other bindings.
 */
@SuppressWarnings("deprecation")
public class TestMetastoreChecking extends AbstractHadoopTestBase {

  private final Path root = new Path("/");

  private URI fsUri;

  private static final String BASE = "s3a://bucket";

  @Before
  public void setup() throws Exception {
    fsUri = new URI(BASE +"/");
  }

  private Configuration chooseStore(String classname) {
    Configuration conf = new Configuration(false);
    if (classname != null) {
      conf.set(S3_METADATA_STORE_IMPL, classname, "code");
    }
    return conf;
  }

  @Test
  public void testNoClass() throws Throwable {
    checkOutcome(null, false);
  }

  @Test
  public void testNullClass() throws Throwable {
    checkOutcome(NULL_METADATA_STORE, true);
  }

  @Test
  public void testLocalStore() throws Throwable {
    checkOutcome(S3GUARD_METASTORE_LOCAL, true);
  }

  @Test
  public void testDDBStore() throws Throwable {
    intercept(PathIOException.class, () ->
        checkOutcome(S3GUARD_METASTORE_DYNAMO, false));
  }

  @Test
  public void testUnknownStore() throws Throwable {
    intercept(PathIOException.class, "unknownStore", () ->
        checkOutcome("unknownStore", false));
  }

  private void checkOutcome(final String classname, final boolean outcome) throws PathIOException {
    Configuration conf = chooseStore(classname);

    Assertions.assertThat(checkNoS3Guard(fsUri, conf))
        .describedAs("check with classname %s", classname)
        .isEqualTo(outcome);
  }
}
