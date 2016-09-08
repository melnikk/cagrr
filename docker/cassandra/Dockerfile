FROM cassandra:2.2

ADD jmxremote.password /etc/cassandra/jmxremote.password
ADD mx4j-tools.jar /usr/share/cassandra/lib/mx4j-tools.jar
RUN chmod 0600 /etc/cassandra/jmxremote.password

EXPOSE 8081
