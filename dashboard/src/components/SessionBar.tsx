import React from "react";
import { Link } from "react-router-dom";

export let SessionBar = (props: { id: string }) => {
  return (
    <div style={{ padding: "1em" }}>
      <Link to={"/session/" + props.id}>Session summary</Link>
      {" | "}
      <Link to={"/session/" + props.id + "/tasklist"}>Task inspector</Link>
      {" | "}
      <Link to={"/session/" + props.id + "/graph"}>Task graph</Link>
    </div>
  );
};
