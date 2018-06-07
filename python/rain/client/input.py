from .data import DataObject, DataType, to_dataobj
from .task import Task


class InputBase:

    dataobj = None
    label = None
    path = None
    load = None
    data_type = None
    content_type = None

    def __init__(self,
                 label=None,
                 path=None,
                 dataobj=None,
                 load=None,
                 content_type=None,
                 write=False):
        assert self.data_type is not None
        if label is not None and not isinstance(label, str):
            raise Exception("Label has to be string, not {!r}".format(label))

        self.label = label
        if path is None:
            if label:
                path = label
            else:
                path = "input_{}".format(id(self))
        self.path = path
        if dataobj is not None:
            dataobj = to_dataobj(dataobj)
            if dataobj.spec.data_type != self.data_type:
                raise Exception(
                    "Input exects data type {}, but provided data object has type {}"
                    .format(self.data_type, dataobj.spec.data_type))
            self.dataobj = dataobj
        self.load = load
        self.content_type = content_type
        self.write = write

    def __repr__(self):
        args = []
        if self.path:
            args.append("path={}".format(self.path))
        if self.dataobj:
            args.append("data={}".format(self.dataobj))
        return "<Input '{}'>".format(self.label, " ".join(args))

    @classmethod
    def _for_data_object(cls, do):
        assert isinstance(do, DataObject)
        if do.spec.data_type == DataType.BLOB:
            c = Input
        else:
            assert do.spec.data_type == DataType.DIRECTORY
            c = InputDir
        return c(label=do.spec.label, dataobj=do, content_type=do.content_type)

    @classmethod
    def _for_program(cls, inp, label=None, execute=False, label_as_path=False):
        """
        Create `Input` from `Input`, `DataObject`, `Task` (single output)
        or `str` for `Program` or `execute`.
        """
        inp0 = inp
        if isinstance(inp, str):
            inp = Input(inp)
        if isinstance(inp, Task):
            inp = inp.output
        if isinstance(inp, DataObject):
            inp = Input._for_data_object(inp)
        if not isinstance(inp, InputBase):
            raise TypeError("Object {!r} cannot be used as input".format(inp0))
        if inp.label is None:
            inp.label = label
        if inp.label is None:
            raise ValueError("Program/execute Inputs need `label`")
        if inp.load is not None:
            raise ValueError("Program/execute Inputs do not accept `load`.")
        if execute and inp.dataobj is None:
                raise(ValueError("`execute` Inputs need `dataobj`"))
        if not execute and inp.dataobj is not None:
                raise(ValueError("`Program` Inputs can't have `dataobj`"))

        if execute and inp.path is None:
            if label_as_path:
                inp.path = inp.label
            else:
                inp.path = "in_{}_{}".format(inp.label, inp.dataobj.id[1])

        return inp


class Input(InputBase):

    data_type = DataType.BLOB


class InputDir(InputBase):

    data_type = DataType.DIRECTORY
