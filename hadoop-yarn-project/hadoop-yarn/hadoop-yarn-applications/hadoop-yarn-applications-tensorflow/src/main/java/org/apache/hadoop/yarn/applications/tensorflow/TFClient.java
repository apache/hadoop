package org.apache.hadoop.yarn.applications.tensorflow;

import java.io.IOException;

/**
 * Created by muzhongz on 16-11-30.
 */
public class TFClient implements Runnable {
    public static final String DEFAULT_TF_CLIENT_PY = "tf_client.py";
    private String clientPy;

    public TFClient(String tfClientName) {

        clientPy = tfClientName;
    }

    @Override
    public void run() {
        Process process = null;
        try {
            process = Runtime.getRuntime().exec("python " + clientPy);
            process.waitFor();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
