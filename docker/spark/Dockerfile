FROM apache/spark:3.5.1-scala2.12-java11-python3-ubuntu

USER root

# verified bang cach truy cap vao container grep hadoop-common nhe
ENV HADOOP_AWS_VERSION=3.3.4
# cung tuong tu z, verified dependencies cua bien HADOOP_AWS_VERSION tren maven nhe
ENV AWS_SDK_VERSION=1.12.262

RUN curl -L https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar -o /opt/spark/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar
RUN curl -L https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar -o /opt/spark/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar

COPY jars/*.jar /opt/spark/jars/

RUN chmod 644 /opt/spark/jars/*.jar

USER spark
