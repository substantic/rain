class RainException(Exception):
    """
    Generic Rain error.
    """
    pass


class RainWarning(Warning):
    """
    Generic Rain warning.
    """
    pass


class SessionException(RainException):
    """
    Session failure
    """
    pass


class TaskException(SessionException):
    """
    Task failure
    """
    pass
