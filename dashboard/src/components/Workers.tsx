import React, { Component } from "react";
import { parseDate } from "../utils/date";
import { fetchEvents } from "../utils/fetch";
import Chart from "./Chart";
import Error from "./Error";

interface Worker {
  name: string;
  x: string;
  columns: string[][] | number;
}

interface State {
  error: string;
  workers: Worker[];
}

class Workers extends Component<{}, State> {
  readonly state: State = {
    error: null,
    workers: []
  };
  private readonly unsubscribe: () => void;

  constructor(props: {}) {
    super(props);
    this.unsubscribe = fetchEvents(
      { event_types: [{ value: "Monitoring", mode: "=" }] },
      events => {
        const state = this.state;
        for (const event of events) {
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
            state.workers.push({
              name: event.event.governor,
              x: "x",
              columns: [["x"], ["CPU %"], ["Mem %"]]
            });
          }

          const governor = this.state.workers[index];

          governor.columns[0].push(parseDate(event.time));

          let sum = 0;
          for (const usage of event.event.cpu_usage) {
            sum += usage;
          }
          governor.columns[1].push(sum / event.event.cpu_usage.length);
          governor.columns[2].push(event.event.mem_usage);
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
