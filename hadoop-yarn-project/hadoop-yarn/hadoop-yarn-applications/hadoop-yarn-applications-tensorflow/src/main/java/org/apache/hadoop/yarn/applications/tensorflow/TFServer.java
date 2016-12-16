package org.apache.hadoop.yarn.applications.tensorflow;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Created by muzhongz on 16-11-29.
 */
public class TFServer implements Runnable {
    private static final Log LOG = LogFactory.getLog(TFServer.class);
    private String tfServerPy;
    private YarnConfiguration conf;

    public static void main(String[] args) {
        LOG.info("start container");
        TFServer server = new TFServer("tf_server.py");
        server.startTFServer();
    }

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

    @Override
    public void run() {
      execCmd("ls -l");
      execCmd("pwd");
      execCmd("python --version");
      String command = "python " + tfServerPy;
      execCmd(command);
    }

    public TFServer(String serverPy) {
      this.tfServerPy = serverPy;
      conf = new YarnConfiguration();
      /*
      for (String c : conf.getStrings(
            YarnConfiguration.YARN_APPLICATION_CLASSPATH,
            YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
        LOG.info(c);
      }
      */
    }

    private String address;
    private int port;
    private ClusterSpec clusterSpec;

    public void startTFServer() {
        Thread thread = new Thread(this);
        thread.start();
    }

    public int stochasticNetport() {
        return 0;
    }
}
