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
    private int tfMasterPort;

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

    @Override
    public void run() {
        execCmd("ls -l");
        if (tfMasterAddress == null || tfMasterPort == 0) {
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

    public void tensorflowServersReady(boolean ready, String masterAddress, int port) {
        LOG.info("tf master : " + masterAddress + ":" + port);

        this.tfMasterAddress = masterAddress;
        this.tfMasterPort = port;
        startTensorflowClient();
    }
}
