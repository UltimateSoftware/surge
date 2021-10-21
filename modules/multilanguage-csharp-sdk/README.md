How to Run !
============

1.Install and start minikube. Note: you'll need Docker too.

```
minikube start --driver=virtualbox --memory=10240 # this can also work without virtualbox
```

2.Install Strimzi. Strimzi is just one of the multiple ways of running a Kafka cluster on K8s.

```
open https://strimzi.io/quickstarts/
```

3.Publish docker images of the C# sample app and of the Surge Multilanguage Server.

```
$ eval $(minikube docker-env)
$ dotnet clean
$ dotnet build
$ dotnet publish -c Release
$ docker build -t dotnetsurgeapp -f Dockerfile .
```


```
$ eval $(minikube docker-env)
$ sbt
sbt:surge> project surge-engine-multilanguage
sbt:surge-engine-multilanguage> docker:publishLocal
```

4.Apply yaml files in the following order:

```
$ kubectl create -f events-topic.yaml -n kafka
$ kubectl create -f state-topic.yaml -n kafka
$ kubectl create -f surge.yaml -n kafka
```

