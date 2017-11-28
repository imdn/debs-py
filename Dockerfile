FROM ubuntu:artful

RUN mkdir /debs2017

WORKDIR /debs2017

RUN apt-get update
RUN apt-get install -y python3.6
RUN apt-get install -y python3-pip
RUN pip3 install --trusted-host pypi.python.org numpy scipy pika
RUN apt-get install -y openjdk-8-jre

# Layer it for easier deployment on Docker
ADD src/ run.sh /debs2017/
COPY metadata /debs2017/metadata
ADD debs-parrotbenchmark-system-1.0-SNAPSHOT.jar /debs2017/

ENV JAVA_VER 8
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64

# DEBS SPECIFIC ENV_VARS

ENV RABBIT_MQ_HOST_NAME_KEY="rabbit"
ENV HOBBIT_SESSION_ID_KEY=exp1
ENV SYSTEM_URI_KEY="http://project-hobbit.eu/resources/debs2017/PyDEBS2017"
ENV SYSTEM_PARAMETERS_MODEL_KEY="{}"
ENV HOBBIT_EXPERIMENT_URI_KEY="http://project-hobbit.eu/resources/debs2017/experiment1"
#ENV HOBBIT_SYSTEM_URI=http://project-hobbit.eu/resources/debs2017/PyDEBS2017
#ENV HOBBIT_RABBIT_HOST=rabbit
#ENV HOBBIT_SESSION_ID=exp1
#ENV SYSTEM_PARAMETERS_MODEL={}
#ENV HOBBIT_EXPERIMENT_URI=http://example.com/exp1

# Run app.py when the container launches
#CMD [ "./run.sh" ]
CMD ["java", "-cp", "debs-parrotbenchmark-system-1.0-SNAPSHOT.jar", "romromov.DebsParrotBenchmarkSystemRunner"]
