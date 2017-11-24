FROM ubuntu:artful

RUN mkdir /debs2017

WORKDIR /debs2017

RUN apt-get update
RUN apt-get install -y python3.6
RUN apt-get install -y python-pip
RUN pip install --trusted-host pypi.python.org numpy scipy pika
RUN apt-get install -y openjdk-8-jre

# Layer it for easier deployment on Docker
ADD src run.sh /debs2017
ADD deployment /debs2017
ADD debs-parrotbenchmark-system-1.0-SNAPSHOT.jar /debs2017

# Run app.py when the container launches
#CMD [ "java -jar debs-parrotbenchmark-system-1.0-SNAPSHOT.jar" ]
#CMD [ "./run.sh" ]
CMD ["java", "-cp", "debs-parrotbenchmark-system-1.0-SNAPSHOT.jar", "romromov.DebsParrotBenchmarkSystemRunner"]
