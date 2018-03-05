# GitHub (Flink) plugin
## Table of contents
1. [Architecture](#architecture)
    - [Overview](#overview)
2. [Data storage](#data-storage)
    - [Data structure](#data-structure)
3. [GitHub requests](#github-requests)
    - [Commit retrieval](#commit-retrieval)
4. [Planning](#planning)
5. [Dependencies](#dependencies)

# Architecture
## Overview
![architecture](architecture.png)
# Data storage
## Data structure
Currently the `PushEvent`s and `Commit`s are stored. See the [GitHubProtocol](https://github.com/codefeedr/codefeedr/blob/github_flink_plugin/src/main/scala/org/codefeedr/Core/Clients/GitHub/GitHubProtocol.scala)
class to see the exact fields. The following (unique) indexes are used:

- PushEvent: `id` - The id uniquely identifies an (Push)Event.
- Commit: `url` - The url contains both the `sha` and the `repo_name` of a commit. 

# GitHub requests
## Commit retrieval
1. Retrieve from MongoDB the latest SHA (commit with latest date from committer/author)
2. If this SHA corresponds with `before` field from PushEvent (and it has less than 20 commits),
then only retrieve commits enclosed in PushEvent.
3. If this SHA doesn't correspond with the `before` field (or commit size is bigger than 20), 
then start retrieving commits SHAs from the `/commits` endpoint. It should start retrieving and storing commits from the `head` of the 
PushEvent until it finds the commits with the same SHA as retrieved from the DB. 

# Planning
The current (rough) planning for this GitHub plugin:
- [ ] Process all event types (PushEvents/IssuesEvent etc.)
- [ ] GitHub plugin management/monitoring?? (a way to manage/monitor all the different jobs in the GitHub plugin)
    - [ ] Keep some (global) plugin statistics (daily events processed, hourly throughput)
    - [ ] API keys in ZooKeeper (add/del keys on runtime)
- [ ] Improved debugging/logging
- [ ] Fully support Avro Schema's
- [ ] Run jobs in parallel (do we actually need this? can't we just run different jobs seperately)
- [ ] Start a job from a historic point using Mongo (is it feasible to stream mongo data into kafka/flink?)
- [ ] Work out Kafka configuration (how long do we keep the data there?)
- [ ] Add more jobs to the plugin (e.g. Issues/PullRequests etc.)

# Dependencies

Currently this plugin depends on the following (external) dependencies:
- [Eclipse Egit](https://github.com/eclipse/egit-github) - Provides connection with the GitHub API
- [MongoDB Scala Driver](http://mongodb.github.io/mongo-scala-driver/2.2/) - Provides connection with MongoDB
- [Json4s](http://json4s.org/) - Extracts JSON data into case classes.
