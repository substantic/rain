import React from "react";

interface Props {
  status: string;
}

export const SessionStatusBadge = (props: Props) => {
  const style: { color?: string } = {};
  if (props.status === "Open") {
    style.color = "green";
  }
  if (props.status === "Error") {
    style.color = "red";
  }
  if (props.status === "Server lost") {
    style.color = "violet";
  }
  return <span style={style}>{props.status}</span>;
};

export const TaskStatusBadge = (props: Props) => {
  const style: { color?: string } = {};
  if (props.status === "running") {
    style.color = "green";
  }
  if (props.status === "error") {
    style.color = "red";
  }
  return <span style={style}>{props.status}</span>;
};

export function niceTime(s: number) {
  if (s < 0.5) {
    return (s * 1000).toFixed(0) + "ms";
  }
  if (s < 90) {
    return s.toFixed(1) + "s";
  }
  return (s / 60).toFixed(0) + "min";
}
