# GitHub (Flink) plugin
## Table of contents
1. [Architecture](#architecture)
    - [Overview](#overview)
2. [Dependencies](#dependencies)

# Architecture
## Overview
![architecture](architecture.png)
# Dependencies

Currently this plugin depends on the following (external) dependencies:
- [Eclipse Egit](https://github.com/eclipse/egit-github) - Provides connection with the GitHub API
- [MongoDB Scala Driver](http://mongodb.github.io/mongo-scala-driver/2.2/) - Provides connection with MongoDB
- [Json4s](http://json4s.org/) - Extracts JSON data into case classes.
