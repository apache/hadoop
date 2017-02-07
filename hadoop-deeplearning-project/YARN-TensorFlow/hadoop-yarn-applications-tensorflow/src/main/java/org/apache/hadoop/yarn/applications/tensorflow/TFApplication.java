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

package org.apache.hadoop.yarn.applications.tensorflow;

public class TFApplication {

    public static final String OPT_TF_APPNAME = "appname";
    public static final String OPT_TF_PRIORITY = "priority";
    public static final String OPT_TF_QUEUE = "queue";
    public static final String OPT_TF_MASTER_MEMORY = "master_memory";
    public static final String OPT_TF_MASTER_VCORES = "master_vcores";
    public static final String OPT_TF_CONTAINER_MEMORY = "container_memory";
    public static final String OPT_TF_CONTAINER_VCORES = "container_vcores";
    public static final String OPT_TF_LOG_PROPERTIES = "log_properties";
    public static final String OPT_TF_ATTEMPT_FAILURES_VALIDITY_INTERVAL = "attempt_failures_validity_interval";
    public static final String OPT_TF_NODE_LABEL_EXPRESSION = "node_label_expression";
    public static final String OPT_TF_CONTAINER_RETRY_POLICY = "container_retry_policy";
    public static final String OPT_TF_CONTAINER_RETRY_ERROR_CODES = "container_retry_error_codes";
    public static final String OPT_TF_CONTAINER_MAX_RETRIES = "container_max_retries";
    public static final String OPT_TF_CONTAINER_RETRY_INTERVAL = "container_retry_interval";

    public static final String OPT_TF_APP_ATTEMPT_ID = "app_attempt_id";

    public static final String OPT_TF_CLIENT = "tf_client";
    public static final String OPT_TF_SERVER_JAR = "tf_serverjar";
    public static final String OPT_TF_WORKER_NUM = "num_worker";
    public static final String OPT_TF_PS_NUM = "num_ps";

    public static String makeOption(String opt, String val) {
        return "--" + opt + " " + val;
    }

}
