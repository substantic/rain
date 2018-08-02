import React, { Component } from 'react';
import {Table} from 'reactstrap';
import { Link } from 'react-router-dom';
import update from 'react-addons-update';

import { fetch_events } from '../utils/fetch';
import Error from './Error.js';
import {StatusBadge} from './utils.js';


class Sessions extends Component {

  constructor(props) {
    super(props);
    this.state = {sessions: []};
    this.unsubscribe = fetch_events({"event_types": [
        {value: "SessionNew", mode: "="},
        {value: "SessionClosed", mode: "="}
    ]}, events => {
        let state = this.state;
        for (let event of events) {
          if (event.event.type === 'SessionNew')
          {
              let session = {
                  id: event.event.session,
                  client: event.event.client,
                  created: event.time,
                  finished: null,
                  status: "Open"
              };
              state = update(state, {sessions: {$push: [session]}});
          } else if (event.event.type === 'SessionClosed') {
              let status = "Closed";
              if (event.event.reason === "Error") {
                status = "Error";
              }
              if (event.event.reason === "ServerLost") {
                status = "Server lost";
              }
              const id = event.event.session;
              state = {
                  sessions: state.sessions.map(s => s.id === id ? {
                      ...s,
                      finished: event.time,
                      status: status,
                  } : s)
              };
            }
        }
        this.setState(state);
    }, error => {
      this.setState(update(this.state, {error: {$set: error}}));
    });
  }

  componentWillUnmount() {
    this.unsubscribe();
  }

  render() {
    return (
        <div>
          <Error error={this.state.error}/>
          <h1>Sessions</h1>

          <Table>
            <thead>
            <tr><th>Session</th><th>Status</th><th>Client</th><th>Created</th><th>Finished</th></tr>
            </thead>
            <tbody>
              {this.state.sessions && this.state.sessions.map(s => {
                return (<tr key={s.id}>
                  <td><Link to={"session/" + s.id}>Session {s.id}</Link></td>
                  <td><StatusBadge status={s.status}/></td>
                  <td>{s.client}</td>
                  <td>{s.created}</td>
                  <td>{s.finished && s.finished}</td>
                  </tr>);
              })}
            </tbody>
          </Table>
        </div>
    );
  }
}

export default Sessions;
