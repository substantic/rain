import React, { Component } from 'react';
import { NavbarBrand } from 'reactstrap';
import { Nav, Navbar, NavItem, NavLink} from 'reactstrap';
import './App.css';
import Sessions from './components/Sessions.js';
import Workers from './components/Workers.js';
import Session from './components/Session.js';
import { Route, BrowserRouter, Switch, Link } from 'react-router-dom';


class App extends Component {

  constructor(props) {
    super(props);
    this.state = {error: null}
  }

  componentDidCatch(error, info) {
    console.log("ERROR");
    console.log(error, info);
    // Display fallback UI
    //this.setState({ hasError: true });
    // You can also log the error to an error reporting service
    //logErrorToMyService(error, info);
  }

  render() {
    return (
      <div className="App">
      <BrowserRouter>
      <div>
        <Navbar>
        <Nav>
          <NavItem><NavLink tag={Link} to="/sessions">Sessions</NavLink></NavItem>
          <NavItem><NavLink tag={Link} to="/workers">Workers</NavLink></NavItem>
        </Nav>
        <NavbarBrand>Rain</NavbarBrand>
        </Navbar>

          <div className="container">
          <Switch>
          <Route path="/session/:id" render={props => <Session id={props.match.params.id}/>} />
          <Route path="/workers" render={() => <Workers/>}/>
          <Route path="/sessions" render={() => <Sessions/>}/>
          <Route path="/" render={() => <Sessions/>}/>
          </Switch>
          </div>
      </div>
      </BrowserRouter>
      </div>
    );
  }
}

export default App;
