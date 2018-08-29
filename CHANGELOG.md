# Changelog

## [Unreleased]

## 0.4.0

### New

- Dashboard improvements (summary, task inspector)
- Task naming for debugging purpose
- Default sessions

### Updates

- Exoscale scripts updated
- Default HTTP port changed to 7222

### Fixes

- Some fixes in error messages and debug outputs

## 0.3.0

### New

- Big renaming (worker -> governor; subworker -> executor)
- Rust tasklib for defining own executors
- C++ tasklib for defining own executors
- Configuration file for governors to define own executors
- Scripts for deploying Rain on exoscale infrastructure
- Dashboard now shows that a session was closed
- Support of Arrow in Python encode()/load()
- Big refactoring of rust packages

### Updates

- Big update of Python API for defining tasks
- Complete rewrite of protocol between governors and executors
- Complete rewrite of task and object attributes

### Fixes

- Fixes in documentation
- Computing usage of memory fixed
- Fixed problem with reusing same logs for new server
- Fixed some corner cases of executor starting
- Disable StrictHostKeyChecking in "rain start"

## 0.2.1

### Fixes

- Some installation problem fixed
- Fixes in documentation
- Fixed removing object in workers
- Fixed "rain start" on remote workers

## 0.2.0

Initial public version
