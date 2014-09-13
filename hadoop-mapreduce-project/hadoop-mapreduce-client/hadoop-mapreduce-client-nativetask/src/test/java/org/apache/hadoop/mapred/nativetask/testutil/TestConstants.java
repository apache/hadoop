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
package org.apache.hadoop.mapred.nativetask.testutil;

public class TestConstants {
  // conf path
  public static final String COMMON_CONF_PATH = "common_conf.xml";
  public static final String COMBINER_CONF_PATH = "test-combiner-conf.xml";
  public static final String KVTEST_CONF_PATH = "kvtest-conf.xml";
  public static final String NONSORT_TEST_CONF = "test-nonsort-conf.xml";
  public static final String COMPRESS_TEST_CONF_PATH = "test-compress-conf.xml";

  // common constants
  public static final String NATIVETASK_TEST_DIR = System.getProperty("test.build.data", "target/test/data");
  public static final String NATIVETASK_COLLECTOR_DELEGATOR = "mapreduce.map.output.collector.delegator.class";
  public static final String NATIVETASK_COLLECTOR_DELEGATOR_CLASS = "org.apache.hadoop.mapred.nativetask.testutil.EnforceNativeOutputCollectorDelegator";
  public static final String NATIVETASK_KVSIZE_MIN = "nativetask.kvsize.min";
  public static final String NATIVETASK_KVSIZE_MAX = "nativetask.kvsize.max";
  public static final String NATIVETASK_KVSIZE_MAX_LARGEKV_TEST = "nativetask.kvsize.max.largekv";

  // kv test
  public static final String FILESIZE_KEY = "kvtest.file.size";
  public static final String NATIVETASK_KVTEST_DIR = NATIVETASK_TEST_DIR + "/kvtest";
  public static final String NATIVETASK_KVTEST_INPUTDIR = NATIVETASK_KVTEST_DIR + "/input";
  public static final String NATIVETASK_KVTEST_NATIVE_OUTPUTDIR = NATIVETASK_KVTEST_DIR + "/output/native";
  public static final String NATIVETASK_KVTEST_NORMAL_OUTPUTDIR = NATIVETASK_KVTEST_DIR + "/output/normal";
  public static final String NATIVETASK_KVTEST_CREATEFILE = "nativetask.kvtest.createfile";
  public static final String NATIVETASK_KVTEST_FILE_RECORDNUM = "nativetask.kvtest.file.recordnum";
  public static final String NATIVETASK_KVTEST_KEYCLASSES = "nativetask.kvtest.keyclasses";
  public static final String NATIVETASK_KVTEST_VALUECLASSES = "nativetask.kvtest.valueclasses";

  // compress test
  public static final String NATIVETASK_COMPRESS_FILESIZE = "nativetask.compress.filesize";
  public static final String NATIVETASK_COMPRESS_TEST_DIR = NATIVETASK_TEST_DIR + "/compresstest";
  public static final String NATIVETASK_COMPRESS_TEST_INPUTDIR = NATIVETASK_COMPRESS_TEST_DIR + "/input";
  public static final String NATIVETASK_COMPRESS_TEST_NATIVE_OUTPUTDIR = NATIVETASK_COMPRESS_TEST_DIR + "/output/native";
  public static final String NATIVETASK_COMPRESS_TEST_NORMAL_OUTPUTDIR = NATIVETASK_COMPRESS_TEST_DIR + "/output/normal";

  // combiner test
  public static final String NATIVETASK_COMBINER_TEST_DIR = NATIVETASK_TEST_DIR + "/combinertest";
  public static final String NATIVETASK_COMBINER_TEST_INPUTDIR = NATIVETASK_COMBINER_TEST_DIR + "/input";
  public static final String NATIVETASK_COMBINER_TEST_NATIVE_OUTPUTDIR = NATIVETASK_COMBINER_TEST_DIR + "/output/native";
  public static final String NATIVETASK_COMBINER_TEST_NORMAL_OUTPUTDIR = NATIVETASK_COMBINER_TEST_DIR + "/output/normal";
  public static final String NATIVETASK_OLDAPI_COMBINER_TEST_NATIVE_OUTPUTPATH = NATIVETASK_COMBINER_TEST_DIR + "/oldapi/output/native";
  public static final String NATIVETASK_OLDAPI_COMBINER_TEST_NORMAL_OUTPUTPATH = NATIVETASK_COMBINER_TEST_DIR + "/oldapi/output/normal";
  public static final String NATIVETASK_COMBINER_WORDCOUNT_FILESIZE = "nativetask.combiner.wordcount.filesize";
  public static final String NATIVETASK_NONSORTTEST_FILESIZE = "nativetask.nonsorttest.filesize";

  // nonsort test
  public static final String NATIVETASK_MAP_OUTPUT_SORT = "mapreduce.sort.avoidance";
  public static final String NATIVETASK_NONSORT_TEST_DIR = NATIVETASK_TEST_DIR + "/nonsorttest";
  public static final String NATIVETASK_NONSORT_TEST_INPUTDIR = NATIVETASK_NONSORT_TEST_DIR + "/input";
  public static final String NATIVETASK_NONSORT_TEST_NATIVE_OUTPUT = NATIVETASK_NONSORT_TEST_DIR + "/output/native";
  public static final String NATIVETASK_NONSORT_TEST_NORMAL_OUTPUT = NATIVETASK_NONSORT_TEST_DIR + "/output/normal";

}
