from io import StringIO
import re
import datetime
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
def ensure_available_space():

    dbx = dropbox.Dropbox(DROPBOX_TOKEN)

    # test if usage threshhold exceeded (below min space)
    # if below, delete last two days of data (subject to change)
    usage = dbx.users_get_space_usage()
    used = usage.used
    allocated = usage.allocation.get_individual().allocated
    remaining_space = allocated - used
    print("remaining space: {}".format(str(remaining_space)))

    # delete any files older than 12 weeks
    min_date = datetime.datetime.now() - datetime.timedelta(days=84)

    # list files
    project_files = dbx.files_list_folder("", recursive=True).entries
    project_files = list(
        filter(lambda entry: type(entry) == dropbox.files.FileMetadata, project_files)
    )

    # list files older than min date
    project_files = list(
        filter(lambda entry: entry.client_modified < min_date, project_files)
    )

    if len(project_files) == 0:
        print("no files to delete")
        return None
    else:
        print(min_date)
        print("deleting {} files".format(str(len(project_files))))

        # delete old files
        # for entry in project_files:
        #     path = entry.path_lower
        #     dbx.files_delete(path)

        return None


if __name__ == "__main__":

    ensure_available_space()
