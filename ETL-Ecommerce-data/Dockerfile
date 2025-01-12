FROM quay.io/astronomer/astro-runtime:12.6.0
USER root

# Install OpenJDK-17
RUN apt update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64/
RUN export JAVA_HOME

# # Copy the JDBC driver
# COPY plugins/postgresql-42.4.4.jar /usr/local/spark/jars/