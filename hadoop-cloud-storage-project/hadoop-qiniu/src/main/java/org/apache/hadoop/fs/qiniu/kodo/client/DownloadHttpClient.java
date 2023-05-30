package org.apache.hadoop.fs.qiniu.kodo.client;

import com.qiniu.common.Constants;
import com.qiniu.storage.Configuration;
import okhttp3.*;
import org.apache.hadoop.util.VersionInfo;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class DownloadHttpClient {

    private final OkHttpClient httpClient;
    private final boolean useNoCacheHeader;

    public DownloadHttpClient(Configuration configuration, boolean useNoCacheHeader) {
        Dispatcher dispatcher = new Dispatcher();
        dispatcher.setMaxRequests(configuration.dispatcherMaxRequests);
        dispatcher.setMaxRequestsPerHost(configuration.dispatcherMaxRequestsPerHost);

        ConnectionPool connectionPool = new ConnectionPool(
                configuration.connectionPoolMaxIdleCount,
                configuration.connectionPoolMaxIdleMinutes,
                TimeUnit.MINUTES
        );

        OkHttpClient.Builder builder = new OkHttpClient.Builder();

        builder.dispatcher(dispatcher);
        builder.connectionPool(connectionPool);

        builder.connectTimeout(configuration.connectTimeout, TimeUnit.SECONDS);
        builder.readTimeout(configuration.readTimeout, TimeUnit.SECONDS);
        builder.writeTimeout(configuration.writeTimeout, TimeUnit.SECONDS);

        if (configuration.proxy != null) {
            builder.proxy(
                    new Proxy(configuration.proxy.type,
                            new InetSocketAddress(
                                    configuration.proxy.hostAddress,
                                    configuration.proxy.port)
                    )
            );
            if (configuration.proxy.user != null && configuration.proxy.password != null) {
                builder.proxyAuthenticator((route, response) -> {
                    String credential = Credentials.basic(configuration.proxy.user, configuration.proxy.password);
                    return response.request().newBuilder().
                            header("Proxy-Authorization", credential).
                            header("Proxy-Connection", "Keep-Alive").build();
                });
            }
        }

        this.httpClient = builder.build();
        this.useNoCacheHeader = useNoCacheHeader;
    }

    private static String userAgent() {
        String javaVersion = "Java/" + System.getProperty("java.version");
        String os = System.getProperty("os.name") + " " + System.getProperty("os.arch") + " " + System.getProperty("os.version");
        final String sdk = "QiniuJava/" + Constants.VERSION;
        String userApp = "Hadoop " + VersionInfo.getVersion();
        return sdk + userApp + " (" + os + ") " + javaVersion;
    }

    public Response get(String url, Map<String, String> headers) throws IOException {
        Request.Builder requestBuilder = new Request.Builder().url(url);
        headers.forEach(requestBuilder::addHeader);
        requestBuilder.addHeader("User-Agent", userAgent());
        Request request = requestBuilder.build();
        return httpClient.newCall(request).execute();
    }

    public InputStream fetch(String url, long offset, int size) throws IOException {
        Map<String, String> header = new HashMap<>();
        header.put("Range", String.format("bytes=%d-%d", offset, offset + size - 1));
        if (useNoCacheHeader) {
            header.put("X-QN-NOCACHE", "1");
        }
        try {
            return Objects.requireNonNull(get(url, header).body()).byteStream();
        } catch (IOException e) {
            throw new IOException("fetch " + url + " failed", e);
        }
    }

}
