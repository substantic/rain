@0xa7976aed3d85b454;

using import "common.capnp".WorkerId;
using import "common.capnp".DataObjectId;
using import "common.capnp".DataObjectType;

struct PullReply {
    # Reply to DataStore.pullData.

    data @0 :Data;
    # Returned data starting at the requested offset. The data may be shorter than
    # requested. The reply includes status of the remaining data.

    union {
        ok @1 :Void;
        # The data is non empty, and stream is not depleted.

        eof @2 :Void;
        # All the data until the end of the stream has been returned
        # Calling "pull" again on Stream returns empty data and eof
    }
}

interface Stream {
    pull @0 (size :UInt64) -> PullReply;

    # TODO: Push API??
    # startPushing(size: UInt64, pushCallback: PushCallback);
}

struct StreamResponse {

    stream @0 :Stream;

    dataobjectType @1 :DataObjectType;
    # Type of streamed object, the main purpose is to check that we are receiving
    # what we realy wants

    union {
        streamWithSize @2 :UInt64;
        # Stream attribute is valid stream, that has known size.

        streamUnknownSize @3 :Void;
        # Stream attribute is valid stream, but the other side does not know the size of stream
        
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
    createStream @0 (id :DataObjectId, path: Text, offset :UInt64) -> StreamResponse;
    # Stream data (or its part), if data object is blob than path has to be
    # empty, if object is directry than empty path means the whole directory,
    # othwerwise some sub-directory or blob in directory can be specified by
    # path. Offset allows to set start of the stream (and possibly skip some
    # prefix of stream).

    listDirectory @1 (id :DataObjectId, path: Text) -> StreamResponse;
    # Create stream that contains listing of directory (TODO: FORMAT?) 'id' has
    # to be id of directory; path may specified sub-directory or blob in the
    # directory. If path is empty than the whole directory is listed
}
