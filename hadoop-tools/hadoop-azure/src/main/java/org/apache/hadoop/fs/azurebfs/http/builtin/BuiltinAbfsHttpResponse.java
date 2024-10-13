package org.apache.hadoop.fs.azurebfs.http.builtin;

import org.apache.hadoop.fs.azurebfs.http.AbfsHttpResponse;

import java.io.InputStream;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;

public class BuiltinAbfsHttpResponse<T> extends AbfsHttpResponse {

    private final HttpResponse<T> httpResponse;

    public BuiltinAbfsHttpResponse(HttpResponse<T> httpResponse) {
        this.httpResponse = httpResponse;
    }

    @Override
    public int getResponseCode() {
        return httpResponse.statusCode();
    }

    @Override
    public String getResponseMessage() {
        return ""; // httpResponse.;
    }

    @Override
    public String getHeaderField(String httpHeader) {
        return httpResponse.headers().firstValue(httpHeader).orElse(null);
    }

    @Override
    public Map<String, List<String>> getHeaderFields() {
        return httpResponse.headers().map();
    }

    @Override
    public long getHeaderFieldLong(String headerName, long defaultValue) {
        return httpResponse.headers().firstValueAsLong(headerName).orElse(defaultValue);
    }

    @Override
    public InputStream getBodyInputStream() {
        return (InputStream) httpResponse.body(); // TOCHECK
    }

}
