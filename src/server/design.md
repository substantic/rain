

### Task states

| W | R | S | State       |T.assigned| W.assigned |T.sched.| W.sched. | W.sched_ready | 
|---|---|---| ----------- | -------- | ---------- | ------ | -------- | ------------- |
|   |   |   | NotAssigned |          |            |        |          |               |
|   |   | x | NotAssigned |          |            | W      | x        |               |
|   | x |   | Ready       |          |            |        |          |               |
|   | x | x | Ready       |          |            | W      | x        | x             |
| x | x | x | Assigned    | W        | x          | W      | x        |               |
| x | x | x | Running     | W        | x          | W      | x        |               |
|   | x | x | Finished    |          |            |        |          |               |

Logical properties:
* W - worker knows about the tsk
* R - all the prerequisites of the task are ready
* S - the scheduler has assigned the task to a worker

 

### Data object states

