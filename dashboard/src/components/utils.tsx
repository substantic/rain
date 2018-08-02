import React from "react";

interface Props {
  status: string;
}

export const StatusBadge = (props: Props) => {
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
