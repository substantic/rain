@0xa7976aed3d85b454;

using import "common.capnp".WorkerId;
using import "common.capnp".DataObjectId;
using import "common.capnp".Error;


struct ReadReply {

    enum Status {
        # The data is non empty, and stream is not depleted.
        ok @0;

        # All the data until the end of the stream has been returned
        # Calling "pull" again on Stream returns empty data and eof
        eof @1;
    }

    # Reply to Reader.read.

    data @0 :Data;
    # Returned data starting at the requested offset. The data may be shorter than
    # requested. The reply includes status of the remaining data.

    status @1 :Status;
}

interface Reader {
    read @0 (size :UInt64) -> ReadReply;

    # TODO: Push API??
    # startPushing(size: UInt64, pushCallback: PushCallback);
}

enum DataType {
    blob @0;
    directory @1;
}

struct ReaderResponse {

    reader @0 :Reader;

    size @1 :Int64;
    # Size of stream, -1 if unknown

    dataType @8 :DataType;

    union {
        ok @2 :Void;
        # Valid reader is returned

        redirect @3 :WorkerId;
        # The data are available at the given worker.
        # Only sent by server to a worker. That worker may answer notHere with certain
        # timing.

        notHere @4 :Void;
        # From worker to worker only. The sender should ask the server for the new
        # location. Server will reply with a redirect or the data itself.

        removed @5 :Void;
        # The DataObject data was removed and will not be available (under normal
        # operation). Server may send this to worker (and then it is usually a bug) or
        # server may send it to client (and then client has likely asked for non-kept
        # object)

        error @6 :Error;
        # Only as response for client

        ignored @7 :Void;
        # Only from server to worker. It is returned when "id" is ignored on server.
        # This can happend when server closes a session, but the worker have not yer received
        # the message about it. The best response of the worker is just the ignore
        # the response and wait for messages that brings deletion of dataobject
    }

}

interface DataStore {
    createReader @0 (id :DataObjectId, path: Text, offset :UInt64) -> ReaderResponse;

    # Create reader for data object (or its part)
    # If data object is blob than 'path' has to be empty.
    # If object is directry than empty 'path' means the whole directory,
    # A sub-directory or blob in the directory can be specified by
    # 'path'. Offset allows to set start of the reader stream (and possibly skip some
    # prefix of stream).

    listDirectory @1 (id :DataObjectId, path: Text) -> ReaderResponse;
    # Create reader stream that contains listing of directory (TODO: FORMAT?)
    # Argument 'id' has to be id of a directory data object;
    # path may specified sub-directory or blob in the
    # directory. If path is empty than the whole directory is listed
}
