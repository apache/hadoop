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

package org.apache.hadoop.mapreduce;

import java.util.HashSet;

import org.apache.hadoop.conf.TestConfigurationFieldsBase;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.ShuffleHandler;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.PathOutputCommitterFactory;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;

/**
 * Unit test class to compare the following MR Configuration classes:
 * <p></p>
 * {@link org.apache.hadoop.mapreduce.MRJobConfig}
 * {@link org.apache.hadoop.mapreduce.MRConfig}
 * {@link org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig}
 * {@link org.apache.hadoop.mapred.ShuffleHandler}
 * {@link org.apache.hadoop.mapreduce.lib.output.FileOutputFormat}
 * {@link org.apache.hadoop.mapreduce.lib.input.FileInputFormat}
 * {@link org.apache.hadoop.mapreduce.Job}
 * {@link org.apache.hadoop.mapreduce.lib.input.NLineInputFormat}
 * {@link org.apache.hadoop.mapred.JobConf}
 * <p></p>
 * against mapred-default.xml for missing properties.  Currently only
 * throws an error if the class is missing a property.
 * <p></p>
 * Refer to {@link org.apache.hadoop.conf.TestConfigurationFieldsBase}
 * for how this class works.
 */
public class TestMapreduceConfigFields extends TestConfigurationFieldsBase {

  @SuppressWarnings("deprecation")
  @Override
  public void initializeMemberVariables() {
    xmlFilename = "mapred-default.xml";
    configurationClasses = new Class[] {
        MRJobConfig.class,
        MRConfig.class,
        JHAdminConfig.class,
        ShuffleHandler.class,
        FileOutputFormat.class,
        FileInputFormat.class,
        Job.class,
        NLineInputFormat.class,
        JobConf.class,
        FileOutputCommitter.class,
        PathOutputCommitterFactory.class
    };

    // Initialize used variables
    configurationPropsToSkipCompare = new HashSet<>();

    // Set error modes
    errorIfMissingConfigProps = true;
    errorIfMissingXmlProps = false;

    // Ignore deprecated MR1 properties in JobConf
    configurationPropsToSkipCompare
            .add(JobConf.MAPRED_JOB_MAP_MEMORY_MB_PROPERTY);
    configurationPropsToSkipCompare
            .add(JobConf.MAPRED_JOB_REDUCE_MEMORY_MB_PROPERTY);

    // Resource type related properties are only prefixes,
    // they need to be postfixed with the resource name
    // in order to take effect.
    // There is nothing to be added to mapred-default.xml
    configurationPropsToSkipCompare.add(
        MRJobConfig.MR_AM_RESOURCE_PREFIX);
    configurationPropsToSkipCompare.add(
        MRJobConfig.MAP_RESOURCE_TYPE_PREFIX);
    configurationPropsToSkipCompare.add(
        MRJobConfig.REDUCE_RESOURCE_TYPE_PREFIX);

    // PathOutputCommitterFactory values
    xmlPrefixToSkipCompare = new HashSet<>();
    xmlPrefixToSkipCompare.add(
        PathOutputCommitterFactory.COMMITTER_FACTORY_SCHEME);
  }

}
