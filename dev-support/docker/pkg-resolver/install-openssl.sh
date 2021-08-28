# RUN apt-get remove --auto-remove openssl \
RUN mkdir -p /opt/openssl-src \
  && curl -L -s -S \
    http://www.openssl.org/source/openssl-1.0.1g.tar.gz \
    -o /opt/openssl.tar.gz \
  && tar xzf /opt/openssl.tar.gz --strip-components 1 -C /opt/openssl-src \
  && cd /opt/openssl-src \
  && ./config --prefix=/opt/openssl \
  && make \
  && make install_sw \
  && cd /root \
  && rm -rf /opt/openssl-src