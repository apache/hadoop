#######
# Install SpotBugs 4.2.2
#######
mkdir -p /opt/spotbugs \
  && curl -L -s -S https://github.com/spotbugs/spotbugs/releases/download/4.2.2/spotbugs-4.2.2.tgz \
    -o /opt/spotbugs.tgz \
  && tar xzf /opt/spotbugs.tgz --strip-components 1 -C /opt/spotbugs \
  && chmod +x /opt/spotbugs/bin/*

#######
# Install Boost 1.72 (1.71 ships with Focal)
#######
# hadolint ignore=DL3003
mkdir -p /opt/boost-library \
  && curl -L https://sourceforge.net/projects/boost/files/boost/1.72.0/boost_1_72_0.tar.bz2/download > boost_1_72_0.tar.bz2 \
  && mv boost_1_72_0.tar.bz2 /opt/boost-library \
  && cd /opt/boost-library \
  && tar --bzip2 -xf boost_1_72_0.tar.bz2 \
  && cd /opt/boost-library/boost_1_72_0 \
  && ./bootstrap.sh --prefix=/usr/ \
  && ./b2 --without-python install \
  && cd /root \
  && rm -rf /opt/boost-library

######
# Install Google Protobuf 3.7.1 (3.6.1 ships with Focal)
######
# hadolint ignore=DL3003
mkdir -p /opt/protobuf-src \
  && curl -L -s -S \
    https://github.com/protocolbuffers/protobuf/releases/download/v3.7.1/protobuf-java-3.7.1.tar.gz \
    -o /opt/protobuf.tar.gz \
  && tar xzf /opt/protobuf.tar.gz --strip-components 1 -C /opt/protobuf-src \
  && cd /opt/protobuf-src \
  && ./configure --prefix=/opt/protobuf \
  && make "-j$(nproc)" \
  && make install \
  && cd /root \
  && rm -rf /opt/protobuf-src

######
# Install pylint and python-dateutil
######
pip3 install pylint==2.6.0 python-dateutil==2.8.1

######
# Install bower
######
# hadolint ignore=DL3008
npm install -g bower@1.8.8

######
# Install hadolint
######
curl -L -s -S \
      https://github.com/hadolint/hadolint/releases/download/v1.11.1/hadolint-Linux-x86_64 \
      -o /bin/hadolint \
 && chmod a+rx /bin/hadolint \
 && shasum -a 512 /bin/hadolint | \
      awk '$1!="734e37c1f6619cbbd86b9b249e69c9af8ee1ea87a2b1ff71dccda412e9dac35e63425225a95d71572091a3f0a11e9a04c2fc25d9e91b840530c26af32b9891ca" {exit(1)}'

######
# Intel ISA-L 2.29.0
######
# hadolint ignore=DL3003,DL3008
mkdir -p /opt/isa-l-src \
  && apt-get -q update \
  && apt-get install -y --no-install-recommends automake yasm \
  && apt-get clean \
  && curl -L -s -S \
    https://github.com/intel/isa-l/archive/v2.29.0.tar.gz \
    -o /opt/isa-l.tar.gz \
  && tar xzf /opt/isa-l.tar.gz --strip-components 1 -C /opt/isa-l-src \
  && cd /opt/isa-l-src \
  && ./autogen.sh \
  && ./configure \
  && make "-j$(nproc)" \
  && make install \
  && cd /root \
  && rm -rf /opt/isa-l-src

