import React, { Component } from 'react';
import * as c3  from 'c3';
import 'c3/c3.css';


class Chart extends Component {

  constructor(props) {
      super(props)
      this.data = ["Unprocessed tasks"]
      this.time = ["x"]
  }

  componentDidMount() {
    console.log(this.element);
    this.chart = c3.generate({
      bindto: this.element,
      size: {
        width: 600,
        height: 150,
      },
      data: {
        x: "x",
        columns: [
          this.data,
          this.time,
        ]
      },

      axis: {
        x: {
          type: "timeseries",
          tick: {
            format: "%H:%M:%S"
        }
        }
      }
    });
  }

  shouldComponentUpdate() {
    return false;
  }

  render() {
    return(<div ref={(r)=>this.element=r}>BAF</div>)
  }
}

export default Chart;
