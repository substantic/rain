import React, { Component } from "react";
import update from "react-addons-update";
import { parseDate } from "../utils/date";
import { fetchEvents } from "../utils/fetch";

import AcyclicGraph from "./AcyclicGraph";
import Chart from "./Chart";
import Error from "./Error";
import { SessionBar } from "./SessionBar";

const UNFIN_FILL = "#ACA793";
const UNFIN_STROKE = "#484537";
const FIN_FILL = "#00CCFF";
const FIN_STROKE = "#0088AA";

interface Props {
  id: string;
}

interface State {
  error: string;
  taskNodes: Map<any, any>;
  objNodes: Map<any, any>;
  unprocessed: {
    version: number;
    x: string;
    columns: Array<Array<string | number | Date>>;
  };
}

class SessionGraph extends Component<Props, State> {
  readonly state: State = {
    error: null,
    taskNodes: new Map(),
    objNodes: new Map(),
    unprocessed: {
      version: 0,
      x: "x",
      columns: [["x"], ["# of unprocessed tasks"]]
    }
  };
  private doEffect: boolean;
  private readonly unsubscribe: () => void;
  private graph: AcyclicGraph;

  constructor(props: Props) {
    super(props);

    this.doEffect = false;

    setTimeout(() => (this.doEffect = true), 500);

    this.unsubscribe = fetchEvents(
      { session: { value: +props.id, mode: "=" } },
      events => {
        for (const event of events) {
          if (event.event.type === "ClientSubmit") {
            this.processSubmit(event);
            this.changeUnprocessed(event.time, event.event.tasks.length);
          }
          if (event.event.type === "TaskFinished") {
            this.processTaskFinished(event);
            this.changeUnprocessed(event.time, -1);
          }
          if (event.event.type === "SessionNew") {
            this.state.unprocessed.columns[1].push(0);
            this.state.unprocessed.columns[0].push(parseDate(event.time));
          }
          if (event.event.type === "SessionClosed") {
            this.unsubscribe();
          }
        }
        this.setState(
          update(this.state, {
            unprocessed: {
              version: { $set: this.state.unprocessed.version + 1 }
            }
          })
        );
      },
      error => {
        this.setState(() => ({ error }));
      }
    );
  }

  changeUnprocessed(time: string, change: number) {
    const u = this.state.unprocessed;
    u.columns[1].push(
      (u.columns[1][u.columns[1].length - 1] as number) + change
    );
    u.columns[0].push(parseDate(time));
  }

  componentWillUnmount() {
    this.unsubscribe();
  }

  processTaskFinished(event: any) {
    const updated = {
      fill: FIN_FILL,
      stroke: FIN_STROKE
    };
    const id = event.event.task[1];
    this.graph.updateNode("t" + id, updated, this.doEffect);
    for (const output of this.state.taskNodes.get(id).outputs) {
      this.graph.updateNode(output.id, updated, this.doEffect);
    }
  }

  processSubmit(event: any) {
    const taskNodes = new Map(this.state.taskNodes);
    const objNodes = new Map(this.state.objNodes);
    const nodes = [];

    for (const obj of event.event.dataobjs) {
      const id = "o" + obj.id[1];
      const node = {
        id,
        type: "box",
        fill: UNFIN_FILL,
        stroke: UNFIN_STROKE,
        inputs: [] as any[],
        outputs: [] as any[]
      };
      nodes.push(node);
      objNodes.set(obj.id[1], node);
    }

    for (const task of event.event.tasks) {
      let inputs;

      if (task.inputs) {
        inputs = task.inputs.map((i: any) => objNodes.get(i.id[1]));
      } else {
        inputs = [];
      }

      let outputs;

      if (task.outputs) {
        outputs = task.outputs.map((o: any) => objNodes.get(o[1]));
      } else {
        outputs = [];
      }

      const node = {
        id: "t" + task.id[1],
        type: "circle",
        label: task.task_type,
        fill: UNFIN_FILL,
        stroke: UNFIN_STROKE,
        inputs,
        outputs
      };

      for (const o of inputs) {
        o.outputs.push(node);
      }

      for (const o of outputs) {
        o.inputs.push(node);
      }

      nodes.push(node);
      taskNodes.set(task.id[1], node);
    }

    for (const o of objNodes.values()) {
      if (o.inputs.length === 0) {
        o.fill = FIN_FILL;
        o.stroke = FIN_STROKE;
      }
    }

    this.graph.addNodes(nodes);
    this.setState({
      taskNodes,
      objNodes
    });
  }

  render() {
    return (
      <div>
        <Error error={this.state.error} />
        <h1>Session {this.props.id}</h1>
        <SessionBar id={this.props.id} />
        <Chart data={this.state.unprocessed} />
        <AcyclicGraph ref={(graph: AcyclicGraph) => (this.graph = graph)} />
      </div>
    );
  }
}

export default SessionGraph;
