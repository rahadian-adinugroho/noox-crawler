from abc import ABC, abstractmethod


class BaseProvider(ABC):
    """docstring for BaseProvider"""
    def __init__(self, data):
        self.data = data
        super(BaseProvider, self).__init__()

    @abstractmethod
    def put(self, data):
        """
        add data to be output.
        """
    @abstractmethod
    def size(self):
        """
        return the size of the loaded data.
        """

    @abstractmethod
    def save(self):
        """execute the output."""
