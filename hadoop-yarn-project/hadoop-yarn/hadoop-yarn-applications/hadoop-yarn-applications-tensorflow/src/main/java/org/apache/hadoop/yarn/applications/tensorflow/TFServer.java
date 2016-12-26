package org.apache.hadoop.yarn.applications.tensorflow;


import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;


/**
 * Created by muzhongz on 16-11-29.
 */
public class TFServer implements Runnable {
    private static final Log LOG = LogFactory.getLog(TFServer.class);
    private String tfServerPy;

    public static final String OPT_CS = "cs";
    public static final String OPT_TI = "ti";
    public static final String OPT_JN = "jn";


    private String clusterSpecString = null;
    private Map<String, List<String>> cluster = null;
    private String jobName = null;
    private int taskIndex = -1;

    // Command line options
    private Options opts;
    public static void main(String[] args) {
        LOG.info("start container");
        TFServer server = new TFServer();
        try {
            try {
                if (!server.init(args)) {
                    LOG.info("init failed!");
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        } catch (ParseException e) {
            LOG.info("parse failed");
            e.printStackTrace();
        }
        server.startTFServer();
    }


    public TFServer() {
        opts = new Options();
        opts.addOption(OPT_CS, true, "tf server cluster spec");
        opts.addOption(OPT_JN, true, "tf job name");
        opts.addOption(OPT_TI, true, "tf task index");
    }

    public boolean init(String[] args) throws ParseException, IOException {

        CommandLine cliParser = new GnuParser().parse(opts, args);

        if (args.length == 0) {
            throw new IllegalArgumentException("No args specified for tf server to initialize");
        }

        if (!cliParser.hasOption(OPT_CS) || !cliParser.hasOption(OPT_JN) || !cliParser.hasOption(OPT_TI)) {
            LOG.error("invalid args for tf server!");
            return false;
        }

        clusterSpecString = ClusterSpec.decodeJsonString(cliParser.getOptionValue(OPT_CS));
        jobName = cliParser.getOptionValue(OPT_JN);
        taskIndex = Integer.parseInt(cliParser.getOptionValue(OPT_TI));
        LOG.info("cs: " + clusterSpecString + "; + jn: " + jobName + "; ti: " + taskIndex);
        cluster = ClusterSpec.toClusterMap(clusterSpecString);
        return true;
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
      String command = tfServerPy;
      execCmd(command);
    }


    private String address;
    private int port;

    public void startTFServer() {
/*        org.tensorflow.bridge.TFServer server = new org.tensorflow.bridge.TFServer(cluster, jobName, taskIndex);
        server.start();*/
    }
}
