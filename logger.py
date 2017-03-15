import logging


class SQLAlchemyHandler(logging.Handler):
    def emit(self, record):
        logging.getLogger("werkzeug")
        print("alikemal")
