package org.apache.hadoop.fs.azurebfs.http.builtin;

import org.apache.hadoop.fs.azurebfs.http.AbfsHttpRequestBuilder;
import org.apache.hadoop.fs.azurebfs.services.AbfsHttpHeader;
import org.apache.hadoop.fs.azurebfs.utils.TimeUtils;
import org.apache.hadoop.security.ssl.DelegatingSSLSocketFactory;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublisher;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BuiltinAbfsHttpRequestBuilder extends AbfsHttpRequestBuilder {

    protected HttpRequest.Builder httpRequestBuilder;

    // Deprecated ... used only for SharedKeyCredentials?
    protected Map<String, List<String>> copyHttpHeaders = new HashMap<>();

    private long connectionTimeMs;


    public BuiltinAbfsHttpRequestBuilder(URL url, String method, List<AbfsHttpHeader> requestHeaders, BodyPublisher bodyPublisher) {
        super(url, method);
        URI uri;
        try {
            uri = url.toURI();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
        this.httpRequestBuilder = HttpRequest.newBuilder()
                .method(method, bodyPublisher)
                .uri(uri);
//        this.connection = openConnection();
//        if (this.connection instanceof HttpsURLConnection) {
//            HttpsURLConnection secureConn = (HttpsURLConnection) this.connection;
//            SSLSocketFactory sslSocketFactory = DelegatingSSLSocketFactory.getDefaultFactory();
//            if (sslSocketFactory != null) {
//                secureConn.setSSLSocketFactory(sslSocketFactory);
//            }
//        }
//
//        this.connection.setConnectTimeout(CONNECT_TIMEOUT);
//        this.connection.setReadTimeout(READ_TIMEOUT);
//
//        this.connection.setRequestMethod(method);

        for (AbfsHttpHeader header : requestHeaders) {
            setRequestProperty(header.getName(), header.getValue());
        }
    }

    @Override
    public void setRequestProperty(String key, String value) {
        // connection.setRequestProperty(key, value);
        httpRequestBuilder.header(key, value);
        // copyHttpHeaders.computeIfAbsent(header.getName(), () -> new ArrayList<>(1)).add(header.getValue()):
        List<String> ls = copyHttpHeaders.get(key);
        if (ls == null) {
            ls = new ArrayList<>(1);
        }
        ls.add(value);
    }

    @Override
    public String getRequestProperty(String key) {
        // return connection.getRequestProperty(key);
        List<String> ls = copyHttpHeaders.get(key);
        return (ls != null && !ls.isEmpty())? ls.get(0) : null;
    }

    @Override
    public Map<String, List<String>> getRequestProperties() {
        return copyHttpHeaders;
    }

    @Override
    public String connectionTimeDetails() {
        StringBuilder sb = new StringBuilder();
        sb.append(",connMs=");
        sb.append(connectionTimeMs);
        return sb.toString();
    }

}
