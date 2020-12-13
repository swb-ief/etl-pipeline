from io import StringIO

from luigi.contrib.dropbox import DropboxTarget
from pipeline.config import DROPBOX_TOKEN
import dropbox


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


# ? ==============================
# ? dropbox_clear_space
# ? Additions re: Dropbox clear space
def ensure_available_space(min_space):

    dbx = dropbox.Dropbox(DROPBOX_TOKEN)

    # ? print usage
    usage = dbx.users_get_space_usage()
    print(usage)

    # ? list files
    for entry in dbx.files_list_folder("/", recursive=True).entries:
        print(entry.name)
        print(dbx.files_get_metadata(entry.name))

    # ? delete file
    # path = ""
    # dbx.files_delete(path)

    return None


if __name__ == "__main__":

    ensure_available_space(1000)
