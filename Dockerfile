FROM ubuntu:artful

RUN mkdir /debs2017

WORKDIR /debs2017

ENV JAVA_VER 8
ENV JAVA_HOME /usr/lib/jvm/java-8-oracle
# DEBS SPECIFIC ENV_VARS
ENV HOBBIT_SYSTEM_URI=http://project-hobbit.eu/resources/debs2017/debsparrotsystemexample
ENV HOBBIT_RABBIT_HOST=rabbit
ENV HOBBIT_SESSION_ID=exp1
ENV SYSTEM_PARAMETERS_MODEL={}
ENV HOBBIT_EXPERIMENT_URI=http://example.com/exp1


RUN apt-get update
RUN apt-get install -y python3.6
RUN apt-get install -y python-pip
RUN pip install --trusted-host pypi.python.org numpy scipy pika
RUN apt-get install -y openjdk-8-jre


ADD ./benchmark_system/debs-parrotbenchmark-system-1.0-SNAPSHOT.jar /debs2017

# Run app.py when the container launches
CMD [ "java -jar debs-parrotbenchmark-system-1.0-SNAPSHOT.jar" ]
