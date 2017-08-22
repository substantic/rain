from .rpc import subworker as rpc_subworker


class ControlImpl(rpc_subworker.SubworkerControl.Server):
    pass
