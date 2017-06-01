from output_providers import BaseProvider
import os
import io
import json


class JsonProvider(BaseProvider):

    def __init__(self, output_file, pretty_print=False, data=None):
        if not isinstance(output_file, io.IOBase) and not isinstance(output_file, str):
            raise TypeError('output_file parameter is expected to be an io interface or str')

        if not isinstance(pretty_print, bool):
            raise TypeError('pretty_print parameter is expected to be bool instance')

        if isinstance(output_file, str):
            self._file = open(output_file, 'w')
        else:
            self._file = output_file

        self.pretty_print = pretty_print
        self.data = data

    def size(self):
        if self.data is not None:
            return len(self.data)
        else:
            return 0

    def put(self, data):
        self.data = data
        return self

    def save(self, data=None):
        if data is None:
            data = self.data
        if len(data) < 1:
            raise ValueError('No data to output!')

        if self.pretty_print:
            json.dump(data, self._file, indent=4)
        else:
            json.dump(data, self._file)


# if __name__ == '__main__':
#     print('Subclass:', issubclass(NooxSqlProvider,
#                                   BaseProvider))
#     print('Instance:', isinstance(NooxSqlProvider(config={'db_url': 'localhost', 'db_username': 'root', 'db_password': '', 'db_name': 'nooxdb'}),
#                                   BaseProvider))
#     b = NooxSqlProvider({'db_url': 'localhost', 'db_username': 'root', 'db_password': '', 'db_name': 'nooxdb'})
#     for sc in BaseProvider.__subclasses__():
#         print(sc.__name__)
#     print(b.config)