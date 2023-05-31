# Private Cloud Configuration

For private cloud users, you need to add or modify some additional configuration items
in `$HADOOP_HOME/etc/core-site.xml` when
configuring Hadoop Qiniu:

```xml

<configuration>
    <property>
        <name>fs.qiniu.customRegion.id</name>
        <value>z0</value>
        <description>
            The id of the custom Region, which will be used as the namespace for subsequent domain name configuration
        </description>
    </property>
    <property>
        <name>fs.qiniu.customRegion.custom.z0.ucServer</name>
        <value>https://uc.qiniuapi.com</value>
        <description>
            The ucServer address of the custom Region, if this item is configured, the subsequent domain name
            configuration will be automatically obtained, and no manual configuration is required
        </description>
    </property>

    <property>
        <name>fs.qiniu.customRegion.custom.z0.rsHost</name>
        <value>rs-z0.qiniuapi.com</value>
        <description>
            The RS domain name is configured, do not add the http(s):// prefix. If you want to enable https, please use
            the configuration item fs.qiniu.useHttps
        </description>
    </property>

    <property>
        <name>fs.qiniu.customRegion.custom.z0.rsfHost</name>
        <value>rsf-z0.qiniuapi.com</value>
        <description>
            The RSF domain name is configured, do not add the http(s):// prefix. If you want to enable https, please use
            the configuration item fs.qiniu.useHttps
        </description>
    </property>

    <property>
        <name>fs.qiniu.customRegion.custom.z0.apiHost</name>
        <value>api.qiniuapi.com</value>
        <description>
            The API domain name is configured, do not add the http(s):// prefix. If you want to enable https, please use
            the configuration item fs.qiniu.useHttps
        </description>
    </property>

    <property>
        <name>fs.qiniu.customRegion.custom.z0.iovipHost</name>
        <value>iovip.qiniuio.com</value>
        <description>
            The IOVIP domain name is configured, do not add the http(s):// prefix. If you want to enable https, please
            use the configuration item fs.qiniu.useHttps
        </description>
    </property>

    <property>
        <name>fs.qiniu.customRegion.custom.z0.srcUpHosts</name>
        <value>up.qiniup.com</value>
        <description>
            The source upload domain name is configured, do not add the http(s):// prefix. If you want to enable
            https, please use the configuration item fs.qiniu.useHttps
        </description>
    </property>
    <property>
        <name>fs.qiniu.customRegion.custom.z0.accUpHosts</name>
        <value>upload.qiniup.com</value>
        <description>
            The accelerated upload domain name is configured, do not add the http(s):// prefix. If you want to enable
            https, please use the configuration item fs.qiniu.useHttps. If there are multiple domain name
            configurations, separate them with commas
        </description>
    </property>
    <property>
        <name>fs.qiniu.customRegion.custom.z0.ioSrcHost</name>
        <value>bucketname.kodo-cn-east-1.qiniucs.com</value>
        <description>
            The source download domain name is configured, do not add the http(s):// prefix. If you want to enable
            https, please use the configuration item fs.qiniu.download.useHttps. You can use the
            fs.qiniu.download.domain to override this domain, and the downloaded file will
            not use this domain of this configuration item.
        </description>
    </property>
    <property>
        <name>fs.qiniu.useHttps</name>
        <value>false</value>
        <description>
            Configure whether to use https for upload and management related domain names, the default is true.
            Private cloud environments usually use http, if necessary, you can configure it to false.
        </description>
    </property>
    <property>
        <name>fs.qiniu.download.useHttps</name>
        <value>false</value>
        <description>
            Configure whether to use https for download related domain names, the default is true.
            Private cloud environments usually use http, if necessary, you can configure it to false.
        </description>
    </property>
</configuration>
```