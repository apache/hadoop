package org.apache.hadoop.yarn.applications.tensorflow;

/**
 * Created by muzhongz on 16-12-27.
 */
public class TFMasterAddress {
    private String address;
    private int port;

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }
}
