import logging

class RequestFilter(logging.Filter):
    def __init__(self, msg_type):
        self.msg_type = msg_type

    def filter(self, record):
        try:
            return record.err_type == self.msg_type
        except AttributeError:
            return False


class RequestFormatter(logging.Formatter):
    def format(self, record):
        record.msg = record.msg.strip()
        return super(RequestFormatter, self).format(record)
