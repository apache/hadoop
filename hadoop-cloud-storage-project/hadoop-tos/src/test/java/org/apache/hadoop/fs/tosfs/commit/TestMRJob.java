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

package org.apache.hadoop.fs.tosfs.commit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.tosfs.conf.ConfKeys;
import org.apache.hadoop.fs.tosfs.conf.TosKeys;
import org.apache.hadoop.fs.tosfs.object.tos.TOS;
import org.apache.hadoop.fs.tosfs.util.ParseUtils;
import org.apache.hadoop.fs.tosfs.util.TestUtility;
import org.junit.jupiter.api.BeforeAll;

import java.io.IOException;

public class TestMRJob extends MRJobTestBase {

  @BeforeAll
  public static void beforeClass() throws IOException {
    // Create the new configuration and set it to the IT Case.
    Configuration newConf = new Configuration();
    newConf.set("fs.defaultFS", String.format("tos://%s", TestUtility.bucket()));
    // Application in yarn cluster cannot read the environment variables from user bash, so here we
    // set it into the config manually.
    newConf.set(ConfKeys.FS_OBJECT_STORAGE_ENDPOINT.key("tos"),
        ParseUtils.envAsString(TOS.ENV_TOS_ENDPOINT, false));
    newConf.set(TosKeys.FS_TOS_ACCESS_KEY_ID,
        ParseUtils.envAsString(TOS.ENV_TOS_ACCESS_KEY_ID, false));
    newConf.set(TosKeys.FS_TOS_SECRET_ACCESS_KEY,
        ParseUtils.envAsString(TOS.ENV_TOS_SECRET_ACCESS_KEY, false));

    MRJobTestBase.setConf(newConf);
    // Continue to prepare the IT Case environments.
    MRJobTestBase.beforeClass();
  }
}
