How to Run !
============

1.Install and start minikube. Note: you'll need Docker too.

```
$ minikube delete # delete any existing cluster
$ minikube start --driver=virtualbox --memory=10240 # this can also work without virtualbox
```

2.Install Strimzi. Strimzi is just one of the multiple ways of running a Kafka cluster on K8s.

```
$ kubectl create namespace kafka
$ kubectl create -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka
$ kubectl apply -f https://strimzi.io/examples/latest/kafka/kafka-persistent-single.yaml -n kafka 
$ kubectl wait kafka/my-cluster --for=condition=Ready --timeout=300s -n kafka  
```

3.Publish docker images of the C# sample app and of the Surge Multilanguage Server.

```
$ eval $(minikube docker-env)
$ sbt
sbt:surge> project surge-engine-multilanguage
sbt:surge-engine-multilanguage> docker:publishLocal
sbt:surge-engine-multilanguage> exit
```

```
$ cd modules/multilanguage-csharp-sdk
$ dotnet clean
$ dotnet build
$ dotnet publish -c Release
$ docker build -t dotnetsurgeapp -f Dockerfile .
```

4.Apply yaml files in the following order:

```
$ cd Sample/
$ kubectl create -f events-topic.yaml -n kafka
$ kubectl create -f state-topic.yaml -n kafka
$ kubectl create -f surge.yaml -n kafka
```

