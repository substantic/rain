import React from 'react';


export let StatusBadge = (props) => {
  let style = {}
  if (props.status === "Open") {
    style.color = "green";
  }
  if (props.status === "Error") {
    style.color = "red";
  }
  if (props.status === "Server lost") {
    style.color = "violet";
  }
  return <span style={style}>{props.status}</span>
}