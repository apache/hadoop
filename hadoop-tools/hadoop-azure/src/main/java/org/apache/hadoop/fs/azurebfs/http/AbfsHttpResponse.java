package org.apache.hadoop.fs.azurebfs.http;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

public abstract class AbfsHttpResponse {

    public abstract int getResponseCode();

    public abstract String getResponseMessage();

    public abstract String getHeaderField(String httpHeader);

    public abstract Map<String, List<String>> getHeaderFields();

    public abstract long getHeaderFieldLong(String headerName, long defaultValue);

    public abstract InputStream getBodyInputStream();
}
