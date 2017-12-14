# REP1: Commit message convetions


## Goals

* Making nice & readable commit messages
* Makes possible some automatic processing (e.g. skipping style changing commits during bisect)


## Format

```
TYPE? SCOPE? Short message

Detailed long message describing what and why the commit happens.
```

### Example of messages

```[test] Added new unit tests for resource manipulations```


```FIX [client] Fixed exeption when submitting uncommited tasks, closes #123```


## Rules

* Short message should not end with period.
* First letter of short message should be capitalised.
* Short message should have <70 characters.
* There should be always an empty line between short and long message.
* Long message is optional.


## Types

* &lt;empty&gt; - New feature / Enhancement
* FIX - a fix of a regression
* RF - refactoring
* STYLE - formating changes, typo corrections
* WIP - Work in progress - This commit will be rebased (or at least renamed)


## Scopes

* [client] / [server] / [worker] / [subworker] - Changes in client, server,  ...
* [backend] # server + worker (+ subworkers)
* [all] - High impact change in many places
* [starter] - Functionality under "rain run" command
* [api] - Interface visible to user, Python API, command-line interface
* [test] - Tests
* [doc] - Documentation
* [tooling] - Build scripts, CI, ...


### Scope rules:

* More scopes can be listed as [api][doc][subworker]
* List only scopes where your commit has a major impact. Scopes that were affected minimally should be ommited. (E.g. a commit containing a big change in the server with a few basic unit tests, then just [server] should be used) 
* If you have commit with major impacts in many places, maybe it is time to refactor and split it to more smaller ones
