import React, { Component } from "react";
import { Table } from "reactstrap";
import { fetchEvents } from "../utils/fetch";
import Error from "./Error";

import { FaCaretDown, FaCaretRight } from "react-icons/fa";
import { parseDate } from "../utils/date";
import { SessionBar } from "./SessionBar";
import { niceTime } from "./utils";

const bgColors = ["white", "#EEE", "#D2D2D2"];

interface Node {
  name: string;
  type: string;

  open?: boolean;

  // Groups
  tasksCount?: number;
  tasksFinished?: number;
  childs?: Node[];
  durationSum?: number;
  durationStdDev?: number;

  // Task
  spec?: any;
  info?: any;
  status?: string;
  startTime?: number;
}

interface State {
  name: string;
  error: string;
  root: Node;
  tasks: Map<number, Node>;
}

interface Props {
  id: string;
}

interface ListItem {
  level: number;
  node: Node;
}

function getColor(value: number) {
  const hue = ((1 - value) * 120).toString(10);
  return ["hsl(", hue, ",100%,75%)"].join("");
}

const GroupRow = (props: {
  level: number;
  node: Node;
  rootDurationSum: number;
  toggleOpen: (node: Node) => void;
}) => {
  const node = props.node;
  const durationRatio = node.durationSum / props.rootDurationSum;
  const toggle = () => props.toggleOpen(props.node);
  return (
    <tr style={{ backgroundColor: bgColors[props.level % 3] }}>
      <td>
        <span
          onClick={toggle}
          style={{ paddingLeft: props.level * 2 + "em", cursor: "pointer" }}
        >
          {node.open ? <FaCaretDown /> : <FaCaretRight />}
          {node.name || <i>Unnamed</i>} {node.spec && node.spec.id[1]}{" "}
        </span>
      </td>
      <td>
        {node.tasksFinished}/{node.tasksCount}
      </td>
      <td>
        {node.tasksFinished > 0 &&
          niceTime(node.durationSum / node.tasksFinished) +
            " ±" +
            niceTime(node.durationStdDev)}
      </td>
      <td
        style={{
          paddingLeft: props.level + 0.5 + "em",
          backgroundColor: getColor(durationRatio)
        }}
      >
        {niceTime(node.durationSum)} ({(durationRatio * 100).toFixed(0)}
        %)
      </td>
    </tr>
  );
};

const TaskRow = (props: {
  level: number;
  node: Node;
  rootDurationSum: number;
  toggleOpen: (node: Node) => void;
}) => {
  const node = props.node;
  let durationRatio = 0;
  let duration;
  if (node.info && node.info.duration) {
    duration = node.info.duration;
    durationRatio = duration / props.rootDurationSum;
  } else {
    duration = null;
  }
  const toggle = () => props.toggleOpen(props.node);
  const main = (
    <tr style={{ backgroundColor: "#eef" }}>
      <td>
        <span
          style={{ paddingLeft: props.level * 2 + "em", cursor: "pointer" }}
          onClick={toggle}
        >
          {node.open ? <FaCaretDown /> : <FaCaretRight />}
          Task {node.name} {node.spec && node.spec.id[1]}
        </span>
      </td>
      <td>{node.status}</td>
      <td>{duration && niceTime(duration)}</td>
      {duration ? (
        <td
          style={{
            paddingLeft: props.level + "em",
            backgroundColor: getColor(durationRatio)
          }}
        >
          {niceTime(duration)} ({(durationRatio * 100).toFixed(0)}
          %)
        </td>
      ) : (
        <td />
      )}
    </tr>
  );
  if (!node.open) {
    return main;
  } else {
    return (
      <>
        {main}
        <tr>
          <td colSpan={5} style={{ paddingLeft: (props.level + 1) * 2 + "em" }}>
            <TaskDetails node={props.node} />
          </td>
        </tr>
      </>
    );
  }
};

const TaskDetails = (props: { node: Node }) => {
  const spec = props.node.spec;
  const inputs = spec.inputs || [];
  const outputs = spec.outputs || [];

  const info = props.node.info;
  return (
    <div>
      <Table>
        <thead>
          <tr>
            <th style={{ width: "10em" }}>Spec name</th>
            <th>Value</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <td>Id</td>
            <td>{spec.id[1]}</td>
          </tr>
          <tr>
            <td>Name</td>
            <td>{spec.name}</td>
          </tr>
          <tr>
            <td>Resources</td>
            <td>
              <pre>{JSON.stringify(spec.resources, null, 2)}</pre>
            </td>
          </tr>
          <tr>
            <td>Task type</td>
            <td>{spec.task_type}</td>
          </tr>
          <tr>
            <td>Task config</td>
            <td>
              <pre>{JSON.stringify(spec.config, null, 2)}</pre>
            </td>
          </tr>
          <tr>
            <td>Inputs</td>
            <td>{inputs.length}</td>
          </tr>
          <tr>
            <td>Outputs</td>
            <td>{outputs.length}</td>
          </tr>
          <tr>
            <td>User</td>
            <td>
              <pre>{JSON.stringify(spec.user, null, 2)}</pre>
            </td>
          </tr>
        </tbody>
      </Table>
      {info && (
        <Table>
          <thead>
            <tr>
              <th style={{ width: "10em" }}>Info name</th>
              <th>Value</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td>Start time</td>
              <td>{info.start_time}</td>
            </tr>
            <tr>
              <td>Duration</td>
              <td>{info.duration && niceTime(info.duration)}</td>
            </tr>
            <tr>
              <td>Governor</td>
              <td>{info.governor}</td>
            </tr>
            <tr>
              <td>Error</td>
              <td>
                <pre>{info.error}</pre>
              </td>
            </tr>
            <tr>
              <td>Debug</td>
              <td>
                <pre>{info.debug}</pre>
              </td>
            </tr>
            <tr>
              <td>User</td>
              <td>
                <pre>{JSON.stringify(spec.user, null, 2)}</pre>
              </td>
            </tr>
          </tbody>
        </Table>
      )}
    </div>
  );
};

class TaskList extends Component<Props, State> {
  readonly state: State = {
    name: null,
    error: null,
    root: this.newGroup("All tasks"),
    tasks: new Map()
  };
  private readonly unsubscribe: () => void;

  constructor(props: Props) {
    super(props);
    this.unsubscribe = fetchEvents(
      { session: { value: +props.id, mode: "=" } },
      events => {
        const state = { ...this.state };
        for (const event of events) {
          const evt = event.event;
          const type = event.event.type;
          if (type === "TaskFinished") {
            const node = state.tasks.get(evt.task[1]);
            node.status = evt.info.error ? "error" : "finished";
            node.info = evt.info;
          } else if (type === "TaskStarted") {
            const node = state.tasks.get(evt.task[1]);
            node.status = "running";
            node.startTime = parseDate(event.time).getTime();
            node.info = evt.info;
          } else if (type === "ClientSubmit") {
            this.processSubmit(event.event.tasks);
          } else if (type === "SessionNew") {
            state.name = evt.spec.name;
          } else if (type === "SessionClosed") {
            this.unsubscribe();
          }
        }
        this.updateCounts(this.state.root);
        this.setState(state);
      },
      error => {
        this.setState(() => ({ error }));
      }
    );
  }

  newGroup(name: string): Node {
    return {
      name,
      type: "g",
      childs: [],
      open: false,

      tasksCount: 0,
      tasksFinished: 0
    };
  }

  ensureGroup(node: Node, parent: Node): Node {
    if (node.type === "g") {
      return node;
    }
    const name = node.name;
    const index = parent.childs.indexOf(node);
    node.name = null;
    const newChild = this.newGroup(name);
    newChild.childs.push(node);
    parent.childs[index] = newChild;
    return newChild;
  }
  /*

  findNode(name: string): Node[] {
    let node = this.state.root;
    const tokens = name.split("/");
    const path = [];
    path.push(node);
    for (const token in tokens) {
      node = node.childs.find(n => token === n.name);
      path.push(node);
    }
    return path;
  }

  findTask(name: string, taskId: number): Node[] {
      let path = this.findNode(name);
      let node = path[path.length - 1];
      if (node.type === "t") {
        return path;
      } else {
        const t = node.childs.find(n => n.spec && n.spec.id[1] == taskId);
        path.push(t);
        return path;
      }
  }*/

  findOrCreateGroup(node: Node, names: string, index: number): Node {
    const name = names[index];
    index += 1;
    let child = node.childs.find(c => c.name === name);

    if (!child) {
      child = this.newGroup(name);
      node.childs.push(child);
    } else if (child.type === "t") {
      child = this.ensureGroup(child, node);
    }
    if (index === names.length) {
      return child;
    } else {
      return this.findOrCreateGroup(child, names, index);
    }
  }

  updateCounts(root: Node) {
    let tasksCount = 0;
    let tasksFinished = 0;
    let durationSum = 0;

    for (const node of root.childs) {
      if (node.type === "g") {
        this.updateCounts(node);
        tasksCount += node.tasksCount;
        tasksFinished += node.tasksFinished;
        durationSum += node.durationSum;
      } else {
        tasksCount += 1;
        if (node.status === "finished" || node.status === "error") {
          tasksFinished += 1;
          durationSum += node.info.duration;
        }
      }
    }

    const avg = durationSum / tasksFinished;
    let devSum = 0;

    for (const node of root.childs) {
      if (node.type === "g" && node.durationStdDev) {
        const diff = node.durationSum / node.tasksFinished - avg;
        devSum +=
          node.tasksFinished *
          (node.durationStdDev * node.durationStdDev + diff * diff);
      } else if (node.status === "finished" || node.status === "error") {
        const diff = node.info.duration - avg;
        devSum += diff * diff;
      }
    }

    root.tasksCount = tasksCount;
    root.tasksFinished = tasksFinished;
    root.durationSum = durationSum;
    root.durationStdDev = Math.sqrt(devSum / tasksFinished);
  }

  processSubmit(tasks: any[]) {
    const root = this.state.root;
    for (const task of tasks) {
      const tokens = (task.name || "").split("/");
      if (tokens.length > 1 && tokens[0] === "") {
        tokens.shift();
      }
      let lastName = tokens.pop();
      let group;
      if (tokens.length === 0) {
        group = root;
      } else {
        group = this.findOrCreateGroup(root, tokens, 0);
      }
      const child = group.childs.find(n => n.name === lastName);
      if (child) {
        group = this.ensureGroup(child, group);
        lastName = null;
      }
      const taskNode: Node = {
        name: lastName,
        type: "t",
        spec: task,
        status: null
      };
      group.childs.push(taskNode);
      this.state.tasks.set(task.id[1], taskNode);
    }
  }

  componentWillUnmount() {
    this.unsubscribe();
  }

  linearize() {
    const output: ListItem[] = [];
    this.linearizeHelper(this.state.root, 0, output);
    return output;
  }

  linearizeHelper(node: Node, level: number, output: ListItem[]) {
    output.push({ level, node });
    if (node.childs && node.open) {
      for (const child of node.childs) {
        this.linearizeHelper(child, level + 1, output);
      }
    }
  }

  toggleOpen = (node: Node) => {
    node.open = !node.open;
    this.setState(this.state);
  };

  render() {
    const state = this.state;
    return (
      <div>
        <Error error={this.state.error} />
        {state.name && (
          <div>
            <h1>
              Session '{state.name}' ({this.props.id})
            </h1>
          </div>
        )}
        <SessionBar id={this.props.id} />
        <Table className="text-left" bordered size="sm">
          <thead>
            <tr>
              <th>Name</th>
              <th>Status</th>
              <th>Task Duration</th>
              <th>Duration sum</th>
            </tr>
          </thead>
          <tbody>
            {this.linearize().map(c => {
              if (c.node.type === "g") {
                return (
                  <GroupRow
                    key={c.node.name}
                    node={c.node}
                    toggleOpen={this.toggleOpen}
                    level={c.level}
                    rootDurationSum={this.state.root.durationSum}
                  />
                );
              } else {
                return (
                  <TaskRow
                    key={c.node.spec.id[1]}
                    node={c.node}
                    level={c.level}
                    rootDurationSum={this.state.root.durationSum}
                    toggleOpen={this.toggleOpen}
                  />
                );
              }
            })}
          </tbody>
        </Table>
      </div>
    );
  }
}

export default TaskList;
