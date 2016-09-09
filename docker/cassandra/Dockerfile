FROM cassandra:2.2.7

ADD jmxremote.password /etc/cassandra/jmxremote.password
ADD jolokia.jar /usr/share/cassandra/lib/jolokia.jar
ADD mx4j-tools.jar /usr/share/cassandra/lib/mx4j-tools.jar
RUN chmod 0600 /etc/cassandra/jmxremote.password

EXPOSE 8080
EXPOSE 8081
