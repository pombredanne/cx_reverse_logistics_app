"""Application Logging."""
import logging
import sys
import os


def logging_setup(sqlalchemy_debug=False):
    """Basic config."""
    if sqlalchemy_debug:
        logging.basicConfig()
        logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
    root = logging.getLogger(os.path.basename(__file__))
    if not root.handlers:
        handler = logging.StreamHandler(sys.stdout)
        root.addHandler(handler)
        root.setLevel(logging.INFO)
        root.propagate = False
        return root
    return root
