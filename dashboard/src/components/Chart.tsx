import c3, { ChartAPI } from "c3";
import "c3/c3.css";
import React, { Component } from "react";

interface Props {
  data: any;
}

export default class Chart extends Component<Props> {
  private chart: ChartAPI;
  private element: HTMLElement;

  componentDidMount() {
    this.chart = c3.generate({
      bindto: this.element,
      size: {
        width: 600,
        height: 150
      },
      axis: {
        x: {
          type: "timeseries",
          tick: {
            format: "%H:%M:%S"
          }
        }
      },
      point: {
        show: false
      },
      data: this.props.data
    });
  }

  componentDidUpdate() {
    this.chart.load(this.props.data);
  }

  componentWillUnmount() {
    this.chart.destroy();
  }

  render() {
    return <div ref={r => (this.element = r)} />;
  }
}
