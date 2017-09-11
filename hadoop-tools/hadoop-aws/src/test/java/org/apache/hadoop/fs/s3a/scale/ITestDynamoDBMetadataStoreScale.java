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

package org.apache.hadoop.fs.s3a.scale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.s3guard.DynamoDBMetadataStore;
import org.apache.hadoop.fs.s3a.s3guard.MetadataStore;

import java.io.IOException;

import static org.junit.Assume.*;
import static org.apache.hadoop.fs.s3a.Constants.*;

/**
 * Scale test for DynamoDBMetadataStore.
 */
public class ITestDynamoDBMetadataStoreScale
    extends AbstractITestS3AMetadataStoreScale {

  @Override
  public MetadataStore createMetadataStore() throws IOException {
    Configuration conf = getFileSystem().getConf();
    String ddbTable = conf.get(S3GUARD_DDB_TABLE_NAME_KEY);
    assumeNotNull("DynamoDB table is configured", ddbTable);
    String ddbEndpoint = conf.get(S3GUARD_DDB_REGION_KEY);
    assumeNotNull("DynamoDB endpoint is configured", ddbEndpoint);

    DynamoDBMetadataStore ms = new DynamoDBMetadataStore();
    ms.initialize(getFileSystem().getConf());
    return ms;
  }
}
