import React, { Component } from 'react';
import update from 'react-addons-update';

import { fetch_events } from '../utils/fetch';
import { parse_date } from '../utils/date';
import Error from './Error.js';
import Chart from './Chart';


class Governors extends Component {

  constructor(props) {
    super(props);
    this.state = {governors: []};
    this.unsubscribe = fetch_events({"event_types": [{value: "Monitoring", mode: "="}]}, event => {
      //console.log("EVENT", event);
      let index = -1;
      let i = 0;
      for (let w of this.state.governors) {
        if (w.name === event.event.governor) {
          index = i;
          break;
        }
        i += 1;
      }

      if (index === -1) {
        index = this.state.governors.length;
        this.setState(update(this.state, {governors: {$push: [{
          name: event.event.governor,
          version: 0,
          x: "x",
          columns: [
            ["x"],
            ["CPU %"],
            ["Mem %"]
          ]
        }]}}));
      }

      // We are abusing immutablity here, but implicit versioning fixes this (performance reasons :( )
      let governor = this.state.governors[index];

      governor.columns[0].push(parse_date(event.time));

      let sum = 0;
      for (let usage of event.event.cpu_usage) {
        sum += usage;
      }
      governor.columns[1].push(sum / event.event.cpu_usage.length);
      governor.columns[2].push(event.event.mem_usage);

      /*if (governor.columns[0].length > 100) {
        governor.columns[0].splice(1, 1);
        governor.columns[1].splice(1, 1);
      }*/

    }, error => {
      this.setState(update(this.state, {error: {$set: error}}));
    }, () => {
      for (let i = 0; i < this.state.governors.length; i++) {
          this.setState(update(this.state, {governors: {[i]: {version: {$set: this.state.governors[i].version + 1}}}}));
      }
    });
  }

  componentWillUnmount() {
    this.unsubscribe();
  }

  render() {
    return (
        <div>
          <Error error={this.state.error}/>
          <h1>Governors</h1>
          {
            this.state.governors.map(w =>
              <div key={w.name}>
                <h2>Governor {w.name}</h2>
                {<Chart data={w}/>}
              </div>
            )
          }
        </div>
    );
  }
}

export default Governors;
