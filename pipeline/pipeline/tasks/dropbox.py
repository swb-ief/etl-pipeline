from io import StringIO

from luigi.contrib.dropbox import DropboxTarget
from pipeline.config import DROPBOX_TOKEN

def dropbox_target(path):
    return DropboxTarget(path, DROPBOX_TOKEN)

def textio2stringio(text_io_wrapper):
    """Converts the contents of a text io wrapper to a StringIO instance

    Pandas doesn't accept a TextIOWrapper because it expect that it will have the name kwarg set, 
    we can't do that since we're using dropbox to store files and the client download content as bytes
    (https://luigi.readthedocs.io/en/stable/_modules/luigi/contrib/dropbox.html#DropboxTarget.temporary_path),
    however pandas accepts an StringIO instances.
    """
    return StringIO(text_io_wrapper.read())

def textio2binary(text_io_wrapper):
    return text_io_wrapper.buffer.read()