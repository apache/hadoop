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

package org.apache.hadoop.fs;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;

public class TestLocal_S3FileContextURI extends FileContextURIBase {

  @Override
  @Before
  public void setUp() throws Exception {
    Configuration S3Conf = new Configuration();
    Configuration localConf = new Configuration();

    S3Conf.set(FS_DEFAULT_NAME_DEFAULT, S3Conf.get("test.fs.s3.name"));
    fc1 = FileContext.getFileContext(S3Conf);
    fc2 = FileContext.getFileContext(localConf);
  }

}
