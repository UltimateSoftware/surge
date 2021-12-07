#!/bin/sh
dotnet clean
dotnet build
dotnet publish -c Release
docker build -t dotnetsurgeapp -f Dockerfile .
