import React, { Component } from "react";
import { Link } from "react-router-dom";
import { Table } from "reactstrap";
import { EventWrapper, SessionSpec } from "../lib/event";
import { parseDate } from "../utils/date";
import { fetchEvents } from "../utils/fetch";
import Error from "./Error";
import { niceTime, SessionStatusBadge } from "./utils";

interface Session {
  id: string;
  client: string;
  created: string;
  finished: any;
  status: string;
  spec: SessionSpec;
}

interface State {
  error: string;
  sessions: Session[];
}

class Sessions extends Component<{}, State> {
  readonly state: State = {
    error: null,
    sessions: []
  };
  private readonly unsubscribe: () => void;

  constructor(props: {}) {
    super(props);

    this.unsubscribe = fetchEvents(
      {
        event_types: [
          { value: "SessionNew", mode: "=" },
          { value: "SessionClosed", mode: "=" }
        ]
      },
      (events: EventWrapper[]) => {
        let state = this.state;
        for (const event of events) {
          if (event.event.type === "SessionNew") {
            const session = {
              id: event.event.session,
              client: event.event.client,
              created: event.time,
              finished: null as any,
              status: "Open",
              spec: event.event.spec
            };
            state = { ...state, sessions: [...state.sessions, session] };
          } else if (event.event.type === "SessionClosed") {
            let status = "Closed";
            if (event.event.reason === "Error") {
              status = "Error";
            }
            if (event.event.reason === "ServerLost") {
              status = "Server lost";
            }
            const id = event.event.session;
            state = {
              ...state,
              sessions: state.sessions.map(
                s =>
                  s.id === id
                    ? {
                        ...s,
                        finished: event.time,
                        status
                      }
                    : s
              )
            };
          }
        }
        this.setState(state);
      },
      (error: string) => {
        this.setState(() => ({ error }));
      }
    );
  }

  componentWillUnmount() {
    this.unsubscribe();
  }

  render() {
    return (
      <div>
        <Error error={this.state.error} />
        <h1>Sessions</h1>

        <Table>
          <thead>
            <tr>
              <th>Id</th>
              <th>Name</th>
              <th>Status</th>
              <th>Client</th>
              <th>Created</th>
              <th>Duration</th>
            </tr>
          </thead>
          <tbody>
            {this.state.sessions &&
              this.state.sessions.map(s => {
                const end = s.finished
                  ? parseDate(s.finished).getTime()
                  : new Date().getTime();
                const duration = (end - parseDate(s.created).getTime()) / 1000;
                return (
                  <tr key={s.id}>
                    <td>{s.id}</td>
                    <td>
                      <Link to={"session/" + s.id}>{s.spec.name}</Link>
                    </td>
                    <td>
                      <SessionStatusBadge status={s.status} />
                    </td>
                    <td>{s.client}</td>
                    <td>{s.created}</td>
                    <td>{niceTime(duration)}</td>
                  </tr>
                );
              })}
          </tbody>
        </Table>
      </div>
    );
  }
}

export default Sessions;
