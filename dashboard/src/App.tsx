import React, { Component, ErrorInfo } from "react";
import {
  BrowserRouter,
  Link,
  Route,
  RouteComponentProps,
  Switch
} from "react-router-dom";
import { Nav, Navbar, NavbarBrand, NavItem, NavLink } from "reactstrap";
import "./App.css";
import Session from "./components/Session";
import SessionGraph from "./components/SessionGraph";
import Sessions from "./components/Sessions";
import TaskList from "./components/TaskList";
import Workers from "./components/Workers";

interface State {
  error: Error;
}

export default class App extends Component<{}, State> {
  readonly state: State = {
    error: null
  };

  componentDidCatch(error: Error, info: ErrorInfo) {
    console.log("ERROR");
    console.log(error, info);
    // Display fallback UI
    // this.setState({ hasError: true });
    // You can also log the error to an error reporting service
    // logErrorToMyService(error, info);
  }

  render() {
    return (
      <div className="App">
        <BrowserRouter>
          <div>
            <Navbar color="light">
              <Nav>
                <NavItem>
                  <NavLink tag={Link} to="/sessions">
                    Sessions
                  </NavLink>
                </NavItem>
                <NavItem>
                  <NavLink tag={Link} to="/workers">
                    Workers
                  </NavLink>
                </NavItem>
              </Nav>
              <NavbarBrand>Rain Dashboard</NavbarBrand>
            </Navbar>
            <div className="container">
              <Switch>
                <Route
                  path="/session/:id/tasklist"
                  render={this.renderTaskList}
                />
                <Route
                  path="/session/:id/graph"
                  render={this.renderSessionGraph}
                />
                <Route path="/session/:id" render={this.renderSession} />
                <Route path="/workers" component={Workers} />
                <Route path="/sessions" component={Sessions} />
                <Route path="/" component={Sessions} />
              </Switch>
            </div>
          </div>
        </BrowserRouter>
      </div>
    );
  }

  renderTaskList = (props: RouteComponentProps<{ id: string }>) => {
    return <TaskList id={props.match.params.id} />;
  };

  renderSession = (props: RouteComponentProps<{ id: string }>) => {
    return <Session id={props.match.params.id} />;
  };
  renderSessionGraph = (props: RouteComponentProps<{ id: string }>) => {
    return <SessionGraph id={props.match.params.id} />;
  };
}
