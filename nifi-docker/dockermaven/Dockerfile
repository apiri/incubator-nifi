# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
#

FROM openjdk:8-jre-alpine
LABEL maintainer "Apache NiFi <dev@nifi.apache.org>"


ARG NIFI_VERSION=1.4.0-SNAPSHOT
ARG NIFI_USER=nifi
ARG NIFI_GROUP=nifi
ARG NIFI_UID=1000
ARG NIFI_GID=1000
ARG NIFI_BINARY=target/docker/nifi-${NIFI_VERSION}-bin.tar.gz
ARG MIRROR=https://archive.apache.org/dist
ARG NIFI_BINARY_URL=/nifi/${NIFI_VERSION}/nifi-${NIFI_VERSION}-bin.tar.gz

ENV NIFI_BASE_DIR=/opt/nifi
ENV NIFI_HOME=${NIFI_BASE_DIR}/nifi-${NIFI_VERSION} \
    NIFI_BINARY_URL=/nifi/$NIFI_VERSION/nifi-$NIFI_VERSION-bin.tar.gz

# Setup NiFi user and group
RUN addgroup -g ${NIFI_GID} ${NIFI_GROUP} && \
    adduser -S -G "$(getent group ${NIFI_GID}| cut -d: -f1)" ${NIFI_USER} && \
    mkdir -p ${NIFI_HOME}/conf/templates && \
    chown -R ${NIFI_USER}:${NIFI_GROUP} ${NIFI_BASE_DIR}

ADD ${NIFI_BINARY} ${NIFI_BASE_DIR}
RUN ls -laR ${NIFI_BASE_DIR}
RUN chown -R ${NIFI_USER}:${NIFI_GROUP} ${NIFI_HOME}

USER ${NIFI_USER}

# Web HTTP Port & Remote Site-to-Site Ports
EXPOSE 8080 8181

WORKDIR ${NIFI_HOME}

# Startup NiFi
ENTRYPOINT ["bin/nifi.sh"]
CMD ["run"]
