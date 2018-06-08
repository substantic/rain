import React, { Component } from 'react';
import update from 'react-addons-update';

import AcyclicGraph from './AcyclicGraph';
import Chart from './Chart';
import { fetch_events } from '../utils/fetch';
import { parse_date } from '../utils/date';
import Error from './Error.js';

const UNFIN_FILL = "#ACA793";
const UNFIN_STROKE = "#484537";
const FIN_FILL = "#00CCFF";
const FIN_STROKE = "#0088AA";


class Session extends Component {

  constructor(props) {
    super(props)
    this.state = {
      task_nodes: new Map(),
      obj_nodes: new Map(),
      unprocessed: {
        version: 0,
        x: "x",
        columns: [
          ["x"],
          ["# of unprocessed tasks"]
        ]
      }
    }
    this.time = ["x"]
    this.do_effect = false;

    setTimeout(() => this.do_effect = true, 500);

    this.unsubscribe = fetch_events({"session": {value: +props.id, mode: "="}}, event => {
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
        this.state.unprocessed.columns[0].push(parse_date(event.time));
      }

    }, error => {
      this.setState(update(this.state, {error: {$set: error}}));
    }, () => {
      this.setState(update(this.state, {unprocessed: {version: {$set: this.state.unprocessed.version + 1}}}));
    });
  }

  changeUnprocessed(time, change) {
    let u = this.state.unprocessed;
    u.columns[1].push(u.columns[1][u.columns[1].length - 1] + change);
    u.columns[0].push(parse_date(time));
  }

  componentWillUnmount() {
    this.unsubscribe();
  }

  processTaskFinished(event) {
    let update = {
      fill: FIN_FILL,
      stroke: FIN_STROKE
    };
    let id = event.event.task[1];
    this.graph.updateNode("t" + id, update, this.do_effect);
    for (let output of this.state.task_nodes.get(id).outputs) {
        this.graph.updateNode(output.id, update, this.do_effect);
    }
  }

  processSubmit(event) {

    let task_nodes = new Map(this.state.task_nodes);
    let obj_nodes = new Map(this.state.obj_nodes);
    let nodes = [];

    for (let obj of event.event.dataobjs) {
      let id = "o" + obj.id[1];
      let node = {
        "id": id,
        "type": "box",
        "fill": UNFIN_FILL,
        "stroke": UNFIN_STROKE,
        inputs: [],
        outputs: [],
      };
      nodes.push(node);
      obj_nodes.set(obj.id[1], node);
    }

    for (let task of event.event.tasks) {
      let inputs = task.inputs.map(i => obj_nodes.get(i.id[1]));
      let outputs = task.outputs.map(o => obj_nodes.get(o[1]));

      let node = {
        "id": "t" + task.id[1],
        "type": "circle",
        "label": task.task_type,
        "fill": UNFIN_FILL,
        "stroke": UNFIN_STROKE,
        inputs: inputs,
        outputs: outputs,
      };

      for (let o of inputs) {
        o.outputs.push(node);
      }

      for (let o of outputs) {
        o.inputs.push(node);
      }

      nodes.push(node);
      task_nodes.set(task.id[1], node);
    }

    for (let o of obj_nodes.values()) {
        if (o.inputs.length === 0) {
          o.fill = FIN_FILL;
          o.stroke = FIN_STROKE;
        }
    }

    this.graph.addNodes(nodes);
    this.setState({
      task_nodes: task_nodes,
      obj_nodes: obj_nodes,
    })
  }


  render() {
    return (
        <div>
          <Error error={this.state.error}/>
          <h1>Session {this.props.id}</h1>
          <Chart data={this.state.unprocessed}/>
          <AcyclicGraph ref={(graph) => this.graph = graph} />
        </div>
    );
  }
}

export default Session;
