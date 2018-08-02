export interface SessionSpec {
  name: string;
}

export interface EventWrapper {
  event: Event;
  time: string;
}

export interface Event {
  type: string;
  reason: string;
  session: string;
  client: string;
  tasks: any[];
  dataobjs: any[];
  message: string;
  governor: string;
  cpu_usage: number[];
  mem_usage: number;
  spec: SessionSpec;
}
