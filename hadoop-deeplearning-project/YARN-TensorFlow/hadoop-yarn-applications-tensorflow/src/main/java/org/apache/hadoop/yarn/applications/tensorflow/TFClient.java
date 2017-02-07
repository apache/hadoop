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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


public class TFClient implements Runnable {

    private static final Log LOG = LogFactory.getLog(TFClient.class);
    public static final String TF_CLIENT_PY = "tf_client.py";
    private String tfClientPy;

    private static final String TF_PY_OPT_WORKERS = "wk";
    private static final String TF_PY_OPT_PSES = "ps";

    private String workers = null;
    private String pses = null;

    private void execCmd(String cmd) {
        Process process = null;
        try {
            LOG.info("cmd is " + cmd);
            process = Runtime.getRuntime().exec(cmd);
        } catch (IOException e) {
            LOG.fatal("cmd running failed", e);
            e.printStackTrace();
        }

        try {
            LOG.info("cmd log--->");
            BufferedReader in = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            while ((line = in.readLine()) != null) {

                LOG.info(line);
                System.out.println(line);
            }
            in.close();
            LOG.info("<---cmd log end");
            process.waitFor();
        } catch (InterruptedException e) {
            LOG.fatal("waiting error ", e);
            e.printStackTrace();
        } catch (IOException e) {
            LOG.info("io exception");
            e.printStackTrace();
        }
    }

    public TFClient(String tfClientPy) {
        this.tfClientPy = tfClientPy;
    }

    public void startTensorflowClient(String clusterSpecJsonString) {
        if (clusterSpecJsonString == null || clusterSpecJsonString.equals("")) {
            return;
        }

        Map<String, List<String>> clusterSpec = null;

        try {
            clusterSpec = ClusterSpec.toClusterMapFromJsonString(clusterSpecJsonString);
        } catch (IOException e) {
            LOG.error("cluster spec is invalid!");
            e.printStackTrace();
            return;
        }

        List<String> workerArray = clusterSpec.get(ClusterSpec.WORKER);
        if (workerArray != null) {
            Iterator<String> it = workerArray.iterator();
            String w = it.next();
            if (w != null) {
                workers = w;
            }

            while (it.hasNext()) {
                workers += "," + it.next();
            }
        }

        List<String> psArray = clusterSpec.get(ClusterSpec.PS);
        if (psArray != null) {
            Iterator<String> it = psArray.iterator();
            String p = it.next();
            if (p != null) {
                pses = p;
            }

            while (it.hasNext()) {
                pses += "," + it.next();
            }
        }

        LOG.info("workers: <" + workers + ">;" + "pses: <" + pses + ">");

        Thread thread = new Thread(this);
        thread.start();

    }

    @Override
    public void run() {
        String cmd = "python " + tfClientPy;

        if (workers != null) {
            cmd +=  " " + TFApplication.makeOption(TF_PY_OPT_WORKERS, "\"" + workers + "\"");
        }

        if (pses != null) {
            cmd += " " + TFApplication.makeOption(TF_PY_OPT_PSES, "\"" + pses + "\"");
        }

        LOG.info("TF client command is [" + cmd + "]");
        execCmd(cmd);
    }
}
