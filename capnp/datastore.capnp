@0xa7976aed3d85b454;

using import "graph.capnp".WorkerId;
using import "graph.capnp".DataObjectId;

struct PullReply {
    # Reply to DataStore.pullData.

    data @0 :Data;
    # Returned data starting at the requested offset. The data may be shorter than
    # requested. The reply includes status of the remaining data.

    union {
        ok @1 :Void;
        # The full requested range has been returned.

        eof @2 :Void;
        # All the data until the end of the data object has been returned (but less than
        # the requested range).

        streamHead @3 :Void;
        # All the data until the current stream head has been returned (but less than
        # the requested range).

        redirect @4 :WorkerId;
        # The data are available at the given worker.
        # Only sent by server to a worker. That worker may answer notHere with certain
        # timing.

        notHere @5 :Void;
        # From worker to worker only. The sender should ask the server for the new
        # location. Server will reply with a redirect or the data itself.

        removed @6 :Void;
        # The DataObject data was removed and will not be available (under normal
        # operation). Server may send this to worker (and then it is usually a bug) or
        # server may send it to client (and then client has likely asked for non-kept
        # object)
    }
}

interface DataStore {
    pullData @0 (id :DataObjectId, offset :UInt64, length :UInt64) -> (reply: PullReply);
    # Request a data block from the given data object.
    # This call will only return when the data is actually available, so it may be
    # pending for a very long time.
}
