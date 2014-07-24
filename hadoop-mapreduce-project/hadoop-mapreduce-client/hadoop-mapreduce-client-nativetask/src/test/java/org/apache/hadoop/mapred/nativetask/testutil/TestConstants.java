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
  public static final String COMBINER_CONF_PATH = "test-combiner-conf.xml";
  public static final String KVTEST_CONF_PATH = "kvtest-conf.xml";
  public static final String NONSORT_TEST_CONF = "test-nonsort-conf.xml";

  public static final String NATIVETASK_KVSIZE_MIN = "nativetask.kvsize.min";
  public static final String NATIVETASK_KVSIZE_MAX = "nativetask.kvsize.max";

  public static final String NATIVETASK_KVTEST_INPUTDIR = "nativetask.kvtest.inputdir";
  public static final String NATIVETASK_KVTEST_OUTPUTDIR = "nativetask.kvtest.outputdir";
  public static final String NATIVETASK_KVTEST_NORMAL_OUTPUTDIR = "normal.kvtest.outputdir";
  public static final String NATIVETASK_KVTEST_CREATEFILE = "nativetask.kvtest.createfile";
  public static final String NATIVETASK_KVTEST_FILE_RECORDNUM = "nativetask.kvtest.file.recordnum";
  public static final String NATIVETASK_KVTEST_KEYCLASSES = "nativetask.kvtest.keyclasses";
  public static final String NATIVETASK_KVTEST_VALUECLASSES = "nativetask.kvtest.valueclasses";
  public static final String NATIVETASK_COLLECTOR_DELEGATOR = "mapreduce.map.output.collector.delegator.class";
  public static final String NATIVETASK_COLLECTOR_DELEGATOR_CLASS = "org.apache.hadoop.mapred.nativetask.testutil.EnforceNativeOutputCollectorDelegator";

  public static final String SNAPPY_COMPRESS_CONF_PATH = "test-snappy-compress-conf.xml";
  public static final String GZIP_COMPRESS_CONF_PATH = "test-gzip-compress-conf.xml";
  public static final String BZIP2_COMPRESS_CONF_PATH = "test-bzip2-compress-conf.xml";
  public static final String DEFAULT_COMPRESS_CONF_PATH = "test-default-compress-conf.xml";
  public static final String LZ4_COMPRESS_CONF_PATH = "test-lz4-compress-conf.xml";
  public static final String NATIVETASK_COMPRESS_FILESIZE = "nativetask.compress.filesize";

  public static final String NATIVETASK_TEST_COMBINER_INPUTPATH_KEY = "nativetask.combinertest.inputpath";
  public static final String NATIVETASK_TEST_COMBINER_INPUTPATH_DEFAULTV = "./combinertest/input";
  public static final String NATIVETASK_TEST_COMBINER_OUTPUTPATH = "nativetask.combinertest.outputdir";
  public static final String NATIVETASK_TEST_COMBINER_OUTPUTPATH_DEFAULTV = "./combinertest/output/native";
  public static final String NORMAL_TEST_COMBINER_OUTPUTPATH = "normal.combinertest.outputdir";
  public static final String NORMAL_TEST_COMBINER_OUTPUTPATH_DEFAULTV = "./combinertest/output/normal";
  public static final String OLDAPI_NATIVETASK_TEST_COMBINER_OUTPUTPATH = "oldAPI.nativetask.combinertest.outputdir";
  public static final String OLDAPI_NORMAL_TEST_COMBINER_OUTPUTPATH = "oldAPI.normal.combinertest.outputdir";
  public static final String NATIVETASK_COMBINER_WORDCOUNT_FILESIZE = "nativetask.combiner.wordcount.filesize";
  public static final String NATIVETASK_NONSORTTEST_FILESIZE = "nativetask.nonsorttest.filesize";

  public static final String COMMON_CONF_PATH = "common_conf.xml";

  public static final String FILESIZE_KEY = "kvtest.file.size";
  public static final String NATIVETASK_KVSIZE_MAX_LARGEKV_TEST = "nativetask.kvsize.max.largekv";

  public static final String NATIVETASK_MAP_OUTPUT_SORT = "mapreduce.sort.avoidance";
  public static final String NONSORT_TEST_INPUTDIR = "nativetask.nonsorttest.inputpath";
  public static final String NONSORT_TEST_NATIVE_OUTPUT = "nonsorttest.native.outputdir";
  public static final String NONSORT_TEST_NORMAL_OUTPUT = "nonsorttest.normal.outputdir";

}
