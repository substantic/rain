import React, { Component } from 'react';
import update from 'react-addons-update';

import AcyclicGraph from './AcyclicGraph';
import Chart from './Chart';
import { fetch_events } from '../utils/fetch';
import Error from './Error.js';

const UNFIN_FILL = "#ACA793";
const UNFIN_STROKE = "#484537";
const FIN_FILL = "#00CCFF";
const FIN_STROKE = "#0088AA";


class Session extends Component {

  constructor(props) {
    super(props)
    this.state = {task_nodes: new Map(), obj_nodes: new Map()}

    this.unprocessed = ["# of unprocessed tasks"]
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
      if (event.event.type === "NewSession") {
        this.unprocessed.push(0);
        this.time.push(Date.parse(event.time));
      }

    }, error => {
      this.setState(update(this.state, {error: {$set: error}}));
    }, () => {
      this.chart.chart.load({
          columns: [
            this.unprocessed,
            this.time
          ]
      });
    });
  }

  changeUnprocessed(time, change) {
    this.unprocessed.push(this.unprocessed[this.unprocessed.length - 1] + change);
    this.time.push(Date.parse(time));
  }

  componentWillUnmount() {
    this.unsubscribe();
  }

  processTaskFinished(event) {
    let update = {
      fill: FIN_FILL,
      stroke: FIN_STROKE
    };
    let id = event.event.task.id;
    this.graph.updateNode("task" + id, update, this.do_effect);
    for (let output of this.state.task_nodes.get(id).outputs) {
        this.graph.updateNode(output, update, this.do_effect);
    }
  }

  processSubmit(event) {

    let task_nodes = new Map(this.state.task_nodes);
    let obj_nodes = new Map(this.state.obj_nodes);
    let nodes = [];

    for (let task of event.event.tasks) {
      let node = {
        "id": "task" + task.id.id,
        "type": "circle",
        "label": task.task_type,
        "fill": UNFIN_FILL,
        "stroke": UNFIN_STROKE,
        outputs: [],
      };
      nodes.push(node);
      task_nodes.set(task.id.id, node);
    }

    for (let obj of event.event.dataobjs) {
      let inputs = [];
      let id = "task" + obj.id.id;
      if (obj.producer) {
          inputs.push(task_nodes.get(obj.producer.id));
          task_nodes.get(obj.producer.id).outputs.push(id);
      }

      let fill, stroke;

      if (inputs.length === 0) {
        fill = FIN_FILL;
        stroke = FIN_STROKE;
      } else {
        fill = UNFIN_FILL;
        stroke = UNFIN_STROKE;
      }

      let node = {
        "id": id,
        "inputs": inputs,
        "type": "box",
        "fill": fill,
        "stroke": stroke
      };
      nodes.push(node);
      obj_nodes.set(obj.id.id, node);
    }

    for (let task of event.event.tasks) {
        task_nodes.get(task.id.id).inputs = task.inputs.map(i => obj_nodes.get(i.id.id));
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
          <Chart ref={(chart) => this.chart = chart}/>
          <AcyclicGraph ref={(graph) => this.graph = graph} />
        </div>
    );
  }
}

export default Session;
