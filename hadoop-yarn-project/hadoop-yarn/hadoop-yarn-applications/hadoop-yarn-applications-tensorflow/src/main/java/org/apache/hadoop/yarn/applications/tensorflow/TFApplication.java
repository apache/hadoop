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

/**
 * Created by muzhongz on 16-12-16.
 */
public class TFApplication {

    public static final String OPT_TF_APPNAME = "appname";
    public static final String OPT_TF_PRIORITY = "priority";
    public static final String OPT_TF_QUEUE = "queue";

    public static final String OPT_TF_CLIENT = "tf_client";
    public static final String OPT_TF_SERVER_JAR = "tf_serverjar";
    //public static final String OPT_TF_SERVER_PY = "tf_serverpy";
    public static final String OPT_TF_WORKER_NUM = "num_worker";
    public static final String OPT_TF_PS_NUM = "num_ps";


    public static String makeOption(String opt, String val) {
        return "--" + opt + " " + val;
    }

}
