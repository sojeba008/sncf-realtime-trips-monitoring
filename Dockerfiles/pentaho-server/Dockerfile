FROM rockylinux:9

RUN dnf install -y unzip java-11-openjdk-headless graphviz fontconfig && dnf clean all

COPY pentaho-server-ce-9.3.0.0-428.zip /opt/
RUN unzip /opt/pentaho-server-ce-9.3.0.0-428.zip -d /opt/ && \
    rm /opt/pentaho-server-ce-9.3.0.0-428.zip && \
    rm /opt/pentaho-server/promptuser.sh && \
    mkdir /opt/schemaspy

COPY tmp/cde.zip /opt/
COPY tmp/dashboard.zip /opt/
COPY pentaho_deploy.sh /opt/
COPY schemaspy-6.2.4.jar /opt/schemaspy/
COPY postgresql-42.2.23.jar /opt/schemaspy/


RUN sed -i 's|<login-show-users-list>true</login-show-users-list>|<login-show-users-list>false</login-show-users-list>|' \
/opt/pentaho-server/pentaho-solutions/system/pentaho.xml && \
sed -i 's|<login-show-sample-users-hint>true</login-show-sample-users-hint>|<login-show-sample-users-hint>false</login-show-sample-users-hint>|' \
/opt/pentaho-server/pentaho-solutions/system/pentaho.xml

EXPOSE 8080
ENTRYPOINT ["/opt/pentaho_deploy.sh"]
