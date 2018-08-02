import React, { Component } from "react";
import update from "react-addons-update";
import { parseDate } from "../utils/date";
import { fetchEvents } from "../utils/fetch";
import Chart from "./Chart";
import Error from "./Error";

interface State {
  error: string;
  workers: any[];
}

class Workers extends Component<{}, State> {
  readonly state: State = {
    error: null,
    workers: []
  };
  private readonly unsubscribe: () => void;

  constructor() {
    super({});
    this.unsubscribe = fetchEvents(
      { event_types: [{ value: "Monitoring", mode: "=" }] },
      events => {
        for (const event of events) {
          // console.log("EVENT", event);
          let index = -1;
          let i = 0;
          for (const w of this.state.workers) {
            if (w.name === event.event.governor) {
              index = i;
              break;
            }
            i += 1;
          }

          if (index === -1) {
            index = this.state.workers.length;
            this.setState(s => ({
              workers: [
                ...s.workers,
                {
                  name: event.event.governor,
                  version: 0,
                  x: "x",
                  columns: [["x"], ["CPU %"], ["Mem %"]]
                }
              ]
            }));
          }

          // We are abusing immutablity here, but implicit versioning fixes this (performance reasons :( )
          const governor = this.state.workers[index];

          governor.columns[0].push(parseDate(event.time));

          let sum = 0;
          for (const usage of event.event.cpu_usage) {
            sum += usage;
          }
          governor.columns[1].push(sum / event.event.cpu_usage.length);
          governor.columns[2].push(event.event.mem_usage);

          /*if (governor.columns[0].length > 100) {
                governor.columns[0].splice(1, 1);
                governor.columns[1].splice(1, 1);
              }*/
        }
        let state = this.state;
        for (let i = 0; i < this.state.workers.length; i++) {
          state = update(state, {
            workers: {
              [i]: { version: { $set: this.state.workers[i].version + 1 } }
            }
          });
        }
        this.setState(state);
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
    return (
      <div>
        <Error error={this.state.error} />
        <h1>Workers</h1>
        {this.state.workers.map(w => (
          <div key={w.name}>
            <h2>Worker {w.name}</h2>
            {<Chart data={w} />}
          </div>
        ))}
      </div>
    );
  }
}

export default Workers;
