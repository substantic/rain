import React, { Component } from 'react';
import {Table} from 'reactstrap';
import { Link } from 'react-router-dom';
import update from 'react-addons-update';

import { fetch_events } from '../utils/fetch';
import Error from './Error.js';


class Sessions extends Component {

  constructor(props) {
    super(props);
    this.state = {sessions: []};
    this.unsubscribe = fetch_events({"event_types": [
        {value: "SessionNew", mode: "="},
        {value: "SessionClose", mode: "="}
    ]}, event => {
        if (event.event.type === 'SessionNew')
        {
            let session = {
                id: event.event.session,
                client: event.event.client,
                created: event.time,
                finished: null
            };
            this.setState(update(this.state, {sessions: {$push: [session]}}));
        }
        else if (event.event.type === 'SessionClose')
        {
            const id = event.event.session;
            this.setState(state => ({
                sessions: state.sessions.map(s => s.id === id ? {
                    ...s,
                    finished: event.time
                } : s)
            }));
        }
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
            <tr><th>Session</th><th>Client</th><th>Created</th><th>Finished</th></tr>
            </thead>
            <tbody>
              {this.state.sessions && this.state.sessions.map(s => {
                return (<tr key={s.id}>
                  <td><Link to={"session/" + s.id}>Session {s.id}</Link></td>
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
