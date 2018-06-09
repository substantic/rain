# Changelog


## [Unreleased]

### New

  * Big renaming (worker -> governor; subworker -> executor)
  * Rust tasklib for defining own executors
  * C++ tasklib for defining own executors
  * Configuration file for governors to define own executors
  * Scripts for deploying Rain on exoscale infrastructure
  * Dashboard now shows that a session was closed
  * Support of Arrow in Python encode()/load()

### Updates

  * Big update of Python API for defining tasks
  * Complete rewrite of protocol between governors and executors
  * Complete rewrite of task and object attributes

### Fixes

  * Fixes in documentation
  * Computing usage of memory fixed
  * Fixed problem with reusing same logs for new server
  * Fixed some corner cases of executor starting
  * Disable StrictHostKeyChecking in "rain start"


## 0.2.1

### Fixes

  * Some installation problem fixed
  * Fixes in documentation
  * Fixed removing object in workers
  * Fixed "rain start" on remote workers


## 0.2.0

Initial public version
