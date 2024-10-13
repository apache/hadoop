package org.apache.hadoop.fs.azurebfs.http.legacy;

import org.apache.hadoop.fs.azurebfs.http.AbfsHttpRequestBuilder;
import org.apache.hadoop.fs.azurebfs.services.AbfsHttpHeader;
import org.apache.hadoop.fs.azurebfs.utils.TimeUtils;
import org.apache.hadoop.security.ssl.DelegatingSSLSocketFactory;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.Map;

public class LegacyAbfsHttpRequestBuilder extends AbfsHttpRequestBuilder {

    protected HttpURLConnection connection;

    private long connectionTimeMs;


    public LegacyAbfsHttpRequestBuilder(URL url, String method, List<AbfsHttpHeader> requestHeaders) throws IOException {
        super(url, method);
        this.connection = openConnection();
        if (this.connection instanceof HttpsURLConnection) {
            HttpsURLConnection secureConn = (HttpsURLConnection) this.connection;
            SSLSocketFactory sslSocketFactory = DelegatingSSLSocketFactory.getDefaultFactory();
            if (sslSocketFactory != null) {
                secureConn.setSSLSocketFactory(sslSocketFactory);
            }
        }

        this.connection.setConnectTimeout(CONNECT_TIMEOUT);
        this.connection.setReadTimeout(READ_TIMEOUT);

        this.connection.setRequestMethod(method);

        for (AbfsHttpHeader header : requestHeaders) {
            setRequestProperty(header.getName(), header.getValue());
        }
    }

    @Override
    public void setRequestProperty(String key, String value) {
        connection.setRequestProperty(key, value);
    }

    @Override
    public String getRequestProperty(String key) {
        return connection.getRequestProperty(key);
    }

    @Override
    public Map<String, List<String>> getRequestProperties() {
        return connection.getRequestProperties();
    }

    /**
     * Open the HTTP connection.
     *
     * @throws IOException if an error occurs.
     */
    private HttpURLConnection openConnection() throws IOException {
        long start = System.nanoTime();
        try {
            return (HttpURLConnection) url.openConnection();
        } finally {
            connectionTimeMs = TimeUtils.elapsedTimeMs(start);
        }
    }

    @Override
    public String connectionTimeDetails() {
        StringBuilder sb = new StringBuilder();
        sb.append(",connMs=");
        sb.append(connectionTimeMs);
        return sb.toString();
    }

}
