import abc


class BaseProvider(metaclass=abc.ABCMeta):
    """docstring for BaseProvider"""
    @abc.abstractmethod
    def put(self, data, immediately=False):
        """
        add data to be output.
        """
    @abc.abstractmethod
    def size(self):
        """
        return the size of the loaded data.
        """

    @abc.abstractmethod
    def save(self):
        """execute the output."""
