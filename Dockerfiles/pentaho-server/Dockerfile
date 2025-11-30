FROM rockylinux:9-minimal AS builder

RUN microdnf install -y \
        unzip \
        zip \
        wget \
        java-11-openjdk-headless \
        graphviz \
        fontconfig \
    && microdnf clean all

WORKDIR /build
ARG PENTAHO_VERSION="9.3.0.0-428"
ARG PENTAHO_ZIP="pentaho-server-ce-${PENTAHO_VERSION}.zip"
ARG PENTAHO_URL="https://github.com/ambientelivre/legacy-pentaho-ce/releases/download/pentaho-server-ce-${PENTAHO_VERSION}/${PENTAHO_ZIP}"
RUN wget -q "${PENTAHO_URL}" -O "${PENTAHO_ZIP}"

COPY restitutions/cde ./cde
COPY restitutions/dashboard ./dashboard

RUN zip -r cde.zip cde && zip -r dashboard.zip dashboard

FROM rockylinux:9-minimal
RUN microdnf install -y unzip java-11-openjdk-headless graphviz fontconfig && microdnf clean all

WORKDIR /opt
COPY --from=builder /build/pentaho-server-ce-*.zip /opt/

RUN unzip /opt/pentaho-server-ce-*.zip -d /opt/ \
    && rm /opt/pentaho-server-ce-*.zip \
    && rm /opt/pentaho-server/promptuser.sh \
    && mkdir /opt/schemaspy

COPY --from=builder /build/cde.zip /opt/
COPY --from=builder /build/dashboard.zip /opt/

COPY pentaho_deploy.sh /opt/
COPY schemaspy-6.2.4.jar /opt/schemaspy/
COPY postgresql-42.2.23.jar /opt/schemaspy/
COPY PUCLogin.jsp /opt/pentaho-server/tomcat/webapps/pentaho/jsp/
COPY index.jsp /opt/pentaho-server/tomcat/webapps/pentaho/

RUN sed -i 's|<login-show-users-list>true</login-show-users-list>|<login-show-users-list>false</login-show-users-list>|' \
        /opt/pentaho-server/pentaho-solutions/system/pentaho.xml \
    && sed -i 's|<login-show-sample-users-hint>true</login-show-sample-users-hint>|<login-show-sample-users-hint>false</login-show-sample-users-hint>|' \
        /opt/pentaho-server/pentaho-solutions/system/pentaho.xml

EXPOSE 8080

ENTRYPOINT ["/opt/pentaho_deploy.sh"]
