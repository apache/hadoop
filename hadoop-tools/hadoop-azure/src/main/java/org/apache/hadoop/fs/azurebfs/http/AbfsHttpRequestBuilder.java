package org.apache.hadoop.fs.azurebfs.http;

import org.apache.hadoop.fs.azurebfs.services.AbfsHttpHeader;

import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.Map;

public abstract class AbfsHttpRequestBuilder {

    protected static final int CONNECT_TIMEOUT = 30 * 1000;
    protected static final int READ_TIMEOUT = 30 * 1000;

    protected final URL url;
    protected final String method;

    public AbfsHttpRequestBuilder(URL url, String method) { // , List<AbfsHttpHeader> requestHeaders
        this.url = url;
        this.method = method;
    }

    public URL getURL() {
        return url;
    }

    public String getMethod() {
        return method;
    }

    public abstract void setRequestProperty(String key, String value);

    public abstract String getRequestProperty(String headerName);

    public abstract Map<String, List<String>> getRequestProperties();

    public abstract String connectionTimeDetails();
}
