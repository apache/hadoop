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

/**
 * Created by muzhongz on 16-11-30.
 */
public class TFClient implements Runnable {

    private static final Log LOG = LogFactory.getLog(TFClient.class);
    public static final String TF_CLIENT_PY = "tf_client.py";
    private String tfClientPy;
    private String tfMasterAddress;
    private int tfMasterPort = TFYarnConstants.INVALID_TCP_PORT;
    private String currentDirectory;
    private static final String OPT_MASTER_ADDRESS = "ma";
    private static final String OPT_MASTER_PORT = "mp";




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
        LOG.info("tf client py script: " + tfClientPy);
        this.tfClientPy = tfClientPy;
    }

    private String makeOption(String opt, String val) {

        if (opt == null || opt.equals("")) {
            return "";
        }

        String lead = "--";

        if (val == null) {
            lead += opt;
            return lead;
        }

        lead += opt;
        lead += " ";
        lead += val;
        return lead;
    }


    @Override
    public void run() {
        execCmd("ls -l");

        if (tfMasterAddress == null || tfMasterPort == TFYarnConstants.INVALID_TCP_PORT) {
            LOG.fatal("invalid master address!");
            execCmd("python " + tfClientPy);
        } else {
            execCmd("python " + tfClientPy + " \"" + tfMasterAddress + "\"" + " " + tfMasterPort);
        }
    }

    public void startTensorflowClient() {
        Thread thread = new Thread(this);
        thread.start();
    }

    public void startTensorflowClient(String masterNode) {
        if (masterNode == null || masterNode.equals("")) {
            return;
        }
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                String cmd = "python " + tfClientPy + " " + masterNode;
                LOG.info("TF client command is [" + cmd + "]");
                execCmd(cmd);
            }
        });
        thread.start();

    }

}
