# README

This repository contains my initial solution prototyped in Python for the ACM Distributed Event-Based Systems Grand Challenge (DEBS GC) 2017. The problem statement can be viewed [here](https://project-hobbit.eu/challenges/debs-grand-challenge/)

Arriving at the solution involves a good grasp of the following concepts:

- Semantic Web data model
	 - Resource Description Framework (RDF) / Turtle serialization
	 - Triple stores
	 - Ontologies (OWL)
- Machine Learning
	- Probability / Markov models
	- Clustering algorithms (k-means)

The following technologies/frameworks were involved in developing and evaluating this solution:

- Python 3.6
- Numpy, Scipy for numerical computations
- RabbitMQ (message broker)
- Docker 

The solution was benchmarked for accuracy and performance on the [HOBBIT](https://project-hobbit.eu/outcomes/hobbit-platform/) platform. Benchmarking of the Python version failed due to problems retrieving messages from the RabbitMQ on the HOBBIT platform, using the recommended client library. While the platform should be ideally language agnostic, the developers/organizers later recommended I use Java since it had been tried and tested on the platform. My subsequent implementation in Java 8, using the same logic, is viewable [here](https://github.com/imdn/debs-java)