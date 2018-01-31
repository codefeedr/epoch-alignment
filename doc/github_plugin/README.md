# GitHub (Flink) plugin
## Table of contents
1. [Architecture](#architecture)
    - [Overview](#overview)
2. [GitHub requests](#github-requests)
3. [Dependencies](#dependencies)

# Architecture
## Overview
![architecture](architecture.png)
# GitHub Requests
## Commit retrieval
1. Retrieve from MongoDB the latest SHA (commit with latest date from committer/author)
2. If this SHA corresponds with `before` field from PushEvent (and it has less than 20 commits),
then only retrieve commits enclosed in PushEvent.
3. If this SHA doesn't correspond with the `before` field (or commit size is bigger than 20), 
then start retrieving commits SHAs from the `/commits` endpoint. It should start retrieving and storing commits from the `head` of the 
PushEvent until it finds the commits with the same SHA as retrieved from the DB. 

# Dependencies

Currently this plugin depends on the following (external) dependencies:
- [Eclipse Egit](https://github.com/eclipse/egit-github) - Provides connection with the GitHub API
- [MongoDB Scala Driver](http://mongodb.github.io/mongo-scala-driver/2.2/) - Provides connection with MongoDB
- [Json4s](http://json4s.org/) - Extracts JSON data into case classes.
