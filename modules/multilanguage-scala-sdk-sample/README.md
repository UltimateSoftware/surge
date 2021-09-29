Surge Multilanguage Sample Application
======================================

How to run
-----------

1. Install and start minikube. Note: you'll need Docker too.
    ```
    minikube start --driver=virtualbox --memory=10240 # this can also work without virtualbox
    ```
2. Install Strimzi. Strimzi is just one of the multiple ways of running a Kafka cluster on K8s.
   ```
   open https://strimzi.io/quickstarts/
   ```


3. Publish docker images.
   ```
   $ eval $(minikube docker-env)
   $ sbt
   sbt:surge> project surge-engine-multilanguage
   sbt:surge-engine-multilanguage> docker:publishLocal
   sbt:surge-engine-multilanguage> project surge-engine-multilanguage-scala-sdk-sample
   sbt:surge-engine-multilanguage-scala-sdk-sample> docker:publishLocal 
   ``` 
   
4. Apply yaml files in the following order:
   ```
   $ kubectl create -f events-topic.yaml -n kafka
   $ kubectl create -f state-topic.yaml -n kafka
   $ kubectl create -f surge.yaml -n kafka 
   ```

5. Call the service:

   ```
   $ minikube service --url business-app-service -n kafka
   $ curl http://192.168.99.109:31516/deposit/22805c58-f9f6-4f81-9ec8-1d8cf819e1ef/20
   # replace 192.168.99.109 with the IP you get from the minikube service --url command 
   ```
   
