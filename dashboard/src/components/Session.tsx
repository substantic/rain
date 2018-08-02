import React, { Component } from "react";
import update from "react-addons-update";
import { Link } from "react-router-dom";
import { Progress, Table } from "reactstrap";
import { fetchEvents } from "../utils/fetch";
import Error from "./Error";
import { StatusBadge } from "./utils";

interface Props {
  id: string;
}

interface State {
  error: string;
  session: any;
  submitCount: number;
  tasksCount: number;
  tasksFinished: number;
  objsCount: number;
  objsFinished: number;
}

class Session extends Component<Props, State> {
  readonly state: State = {
    error: null,
    session: null,
    submitCount: 0,
    tasksCount: 0,
    tasksFinished: 0,
    objsCount: 0,
    objsFinished: 0
  };
  private readonly unsubscribe: () => void;

  constructor(props: Props) {
    super(props);
    this.unsubscribe = fetchEvents(
      { session: { value: +props.id, mode: "=" } },
      events => {
        let state = this.state;
        for (const event of events) {
          if (event.event.type === "TaskFinished") {
            state = update(state, {
              tasks_finished: { $set: (state.tasksFinished += 1) }
            });
          }
          if (event.event.type === "ClientSubmit") {
            state = update(state, {
              submit_count: { $set: (state.submitCount += 1) },
              tasks_count: {
                $set: (state.tasksCount += event.event.tasks.length)
              },
              objs_count: {
                $set: (state.objsCount += event.event.dataobjs.length)
              }
            });
          } else if (event.event.type === "SessionNew") {
            const session = {
              client: event.event.client,
              created: event.time,
              finished: null as any,
              status: "Open",
              message: "",
              spec: event.event.spec
            };
            state = { ...state, session };
          } else if (event.event.type === "SessionClosed") {
            let status = "Closed";
            if (event.event.reason === "Error") {
              status = "Error";
            }
            if (event.event.reason === "ServerLost") {
              status = "Server lost";
            }
            state = update(state, {
              session: {
                status: { $set: status },
                message: { $set: event.event.message }
              }
            });
            this.unsubscribe();
          }
        }
        this.setState(state);
        // this.setState(update(this.state, {unprocessed: {version: {$set: this.state.unprocessed.version + 1}}}))
      },
      error => {
        this.setState(() => ({ error }));
      }
    );
  }

  componentWillUnmount() {
    this.unsubscribe();
  }

  render() {
    const state = this.state;
    const session = state.session;
    const taskProgress = 100 * (state.tasksFinished / state.tasksCount);
    return (
      <div>
        <Error error={this.state.error} />
        {session && (
          <div>
            <h1>
              Session '{state.session.spec.name}' ({this.props.id})
            </h1>

            <Table bordered>
              {/*<thead>
              <tr><th>Key</th><th>Value</th><th>Client</th><th>Created</th><th>Finished</th></tr>
              </thead>*/}
              <tbody>
                <tr>
                  <td>Status</td>
                  <td>
                    <StatusBadge status={session.status} />
                    <p className="text-left text-monospace">
                      {session.message}
                    </p>
                  </td>
                </tr>
                <tr>
                  <td>Submits</td>
                  <td>{state.submitCount}</td>
                </tr>
                <tr>
                  <td>Tasks</td>
                  <td>
                    <div className="text-center">
                      {state.tasksFinished}/{state.tasksCount} (
                      {taskProgress.toFixed(1)}
                      %)
                    </div>
                    <Progress value={taskProgress} />
                  </td>
                </tr>
                <tr>
                  <td>Data Objects</td>
                  <td>{state.objsCount}</td>
                </tr>
                <tr>
                  <td>Client</td>
                  <td>{session.client}</td>
                </tr>
              </tbody>
            </Table>
          </div>
        )}
        <Link to={"/session/" + this.props.id + "/graph"}>Session Graph</Link>
      </div>
    );
  }
}

export default Session;
