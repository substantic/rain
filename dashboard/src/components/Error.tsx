import React from "react";
import { Alert } from "reactstrap";

interface Props {
  error: string;
}

const Error = (props: Props) => {
  return (
    <div>{props.error && <Alert color="danger">{props.error}</Alert>}</div>
  );
};

export default Error;
