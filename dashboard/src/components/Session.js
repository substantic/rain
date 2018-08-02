import React, { Component } from 'react';
import update from 'react-addons-update';

import AcyclicGraph from './AcyclicGraph';
import Chart from './Chart';
import { fetch_events } from '../utils/fetch';
import { parse_date } from '../utils/date';
import Error from './Error.js';
import {Table, Progress} from 'reactstrap';
import { StatusBadge } from './utils';
import { Link } from 'react-router-dom';


const UNFIN_FILL = "#ACA793";
const UNFIN_STROKE = "#484537";
const FIN_FILL = "#00CCFF";
const FIN_STROKE = "#0088AA";


class Session extends Component {

  constructor(props) {
    super(props)
    this.state = {session: null,
                  submit_count: 0,
                  tasks_count: 0,
                  tasks_finished: 0,
                  objs_count: 0,
                  objs_finished: 0}
    this.unsubscribe = fetch_events({"session": {value: +props.id, mode: "="}}, events => {
      let state = this.state;
      for (let event of events) {
        if (event.event.type === 'TaskFinished') {
          state = update(state,
            {tasks_finished: {$set: state.tasks_finished += 1}});
        }
        if (event.event.type === 'ClientSubmit') {
          state = update(state,
            {submit_count: {$set: state.submit_count += 1},
             tasks_count: {$set: state.tasks_count += event.event.tasks.length},
             objs_count: {$set: state.objs_count += event.event.dataobjs.length}});
        } else if (event.event.type === 'SessionNew')
        {
            let session = {
                client: event.event.client,
                created: event.time,
                finished: null,
                status: "Open",
                message: "",
            };
            state = update(state, {session: {$set: session}});
        } else if (event.event.type === "SessionClosed") {
          let status = "Closed";
          if (event.event.reason === "Error") {
            status = "Error";
          }
          if (event.event.reason === "ServerLost") {
            status = "Server lost";
          }
          state = update(state,
            {session: {status: {$set: status},
                       message: {$set: event.event.message}}});
          this.unsubscribe();
        }
      }
      this.setState(state);
      //this.setState(update(this.state, {unprocessed: {version: {$set: this.state.unprocessed.version + 1}}}))
    }, error => {
      this.setState(update(this.state, {error: {$set: error}}));
    });
  }

  componentWillUnmount() {
    this.unsubscribe();
  }

  render() {
    let state = this.state;
    let session = state.session;
    let task_progress = 100 * (state.tasks_finished / state.tasks_count);
    //let obj_progress = 100 * (state.objs_finished / state.objs_count);
    return (
        <div>
          <Error error={this.state.error}/>
          <h1>Session {this.props.id}</h1>
          { session &&
            <Table bordered>
              {/*<thead>
              <tr><th>Key</th><th>Value</th><th>Client</th><th>Created</th><th>Finished</th></tr>
              </thead>*/}
              <tbody>
                <tr><td>Status</td><td><StatusBadge status={session.status}/>
                                        <p className="text-left text-monospace">{session.message}</p></td></tr>
                <tr><td>Submits</td><td>{state.submit_count}</td></tr>
                <tr><td>Tasks</td><td>
                  <div className="text-center">{state.tasks_finished}/{state.tasks_count} ({task_progress.toFixed(1)}%)</div>
                  <Progress value={task_progress}/></td></tr>
                  <tr><td>Data Objects</td>
                  <td>{state.objs_count}</td></tr>
                <tr><td>Client</td><td>{session.client}</td></tr>

              </tbody>
            </Table>
          }
          <Link to={"/session/" + this.props.id + "/graph"}>Session Graph</Link>
        </div>
    );
  }
}

export default Session;
