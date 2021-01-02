import configparser
import os

_config = None

THIS_DIR = os.path.dirname(os.path.abspath(__file__))


def get_config():
    """ loads the configuration associated with the backend

    :remarks: currently also stores some configuration parameters for luigi tasks like start_date
    """
    global _config

    if _config is None:
        path = os.path.join(THIS_DIR, '../backend.ini')
        if not os.path.exists(path):
            raise ValueError(f'Could not read {path}')

        config = configparser.ConfigParser()
        config.read(path)
        _config = config

    return _config
