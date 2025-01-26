/*
 * ByteDance Volcengine EMR, Copyright 2022.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.tosfs.commit.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.tosfs.util.TestUtility;

public class TestCommitter extends CommitterTestBase {
  @Override
  protected Configuration newConf() {
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", String.format("tos://%s", TestUtility.bucket()));
    return conf;
  }
}
