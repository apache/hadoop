package org.apache.hadoop.yarn.applications.tensorflow;

/**
 * Created by muzhongz on 16-12-16.
 */
public class TFApplication {
    public static final String OPT_TF_CLIENT = "tf_client";
    public static final String OPT_TF_CLIENT_PY = "tf_clientpy";
    public static final String OPT_TF_SERVER_JAR = "tf_serverjar";
    public static final String OPT_TF_SERVER_PY = "tf_serverpy";


    public static String makeOption(String opt, String val) {
        return "--" + opt + " " + val;
    }

    public static TFApplicationRpc getRpc() {
        return null;
    }

    public static boolean startRpcService() {
        return true;
    }
}
