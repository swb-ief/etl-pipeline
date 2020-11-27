from io import StringIO

from luigi.contrib.gcs import GCSTarget
from pipeline.config import GCS_TOKEN


def gcloud_target(path):
    return GCSTarget(path, GCS_TOKEN)


def textio2stringio(text_io_wrapper):
    """Converts the contents of a text io wrapper to a StringIO instance

    Pandas doesn't accept a TextIOWrapper because it expect that it will have the name kwarg set,
    we can't do that since we're using GCS to store files and the client download content as bytes
    (https://luigi.readthedocs.io/en/stable/_modules/luigi/contrib/gcs.html#GCSTarget.temporary_path),
    however pandas accepts an StringIO instances.
    """
    return StringIO(text_io_wrapper.read())


def textio2binary(text_io_wrapper):
    return text_io_wrapper.buffer.read()
