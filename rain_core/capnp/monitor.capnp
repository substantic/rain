@0x9811c28d858a5aa4;

struct MonitoringFrames {
    frames @0 :List(Frame);
}

struct Frame {
    timestamp @0 :UInt64;
    cpuUsage @1 :List(UInt8);
    memUsage @2 :UInt8;
}