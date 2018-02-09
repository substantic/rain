import React, { Component } from 'react';
import * as c3  from 'c3';
import 'c3/c3.css';


class Chart extends Component {

  componentDidMount() {
    this.chart = c3.generate({
      bindto: this.element,
      size: {
        width: 600,
        height: 150,
      },
      axis: {
        x: {
          type: "timeseries",
          tick: {
            format: "%H:%M:%S"
          }
        }
      },
      data: this.props.data,
    })
  }

  shouldComponentUpdate(nextProps, nextState) {
    //return this.props.data.version !== nextProps.data.version;
    return true;
  }

  componentDidUpdate(prevProps, prevState) {
    this.chart.load(this.props.data);
  }

  componentWillUnmount() {
    this.chart.destroy();
  }

  render() {
    return(<div ref={(r)=>this.element=r}></div>)
  }
}

export default Chart;
