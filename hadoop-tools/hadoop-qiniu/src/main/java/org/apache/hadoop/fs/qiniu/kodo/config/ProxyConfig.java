package org.apache.hadoop.fs.qiniu.kodo.config;

import org.apache.hadoop.conf.Configuration;

import java.net.Proxy;

public class ProxyConfig extends AConfigBase {
    public final boolean enable;
    public final String hostname;
    public final int port;
    public final String username;
    public final String password;
    public final Proxy.Type type;

    public ProxyConfig(Configuration conf, String namespace) {
        super(conf, namespace);
        this.enable = enable();
        this.hostname = hostname();
        this.port = port();
        this.username = username();
        this.password = password();
        this.type = type();
    }

    private boolean enable() {
        return conf.getBoolean(namespace + ".enable", false);
    }

    private String hostname() {
        return conf.get(namespace + ".hostname", "127.0.0.1");
    }

    private int port() {
        return conf.getInt(namespace + ".port", 8080);
    }

    private String username() {
        return conf.get(namespace + ".username");
    }

    private String password() {
        return conf.get(namespace + ".password");
    }

    private Proxy.Type type() {
        String typeStr = conf.get(namespace + ".type");
        if (typeStr == null) {
            return Proxy.Type.HTTP;
        }
        return Proxy.Type.valueOf(typeStr);
    }
}
