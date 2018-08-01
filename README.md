This application consumes messages from kafka topic. Make sure Kafka and zookeper are up and running on the machine we want to run kafka consumer.

Instructions for Kafka setup : http://kafka.apache.org/documentation.html#quickstart

Build the Jar : gradle clean build fatjar -x test

Run the application :

java -jar [jarForApplication] [topicName] [groupName]

We need to pass 2 command line arguements to start the application.

Ex : java -jar build/9984d94/kafka-consumer-9984d94-all-1.0-SNAPSHOT.jar hari group1

After you start the application, you will see the messages in console that are sent from producer application. You can type exit anytime to quit the consumer.