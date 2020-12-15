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


# ==============================
# dropbox_clear_space
# Additions re: Dropbox clear space
def ensure_available_space():

    dbx = dropbox.Dropbox(DROPBOX_TOKEN)

    # test if usage threshhold exceeded (below min space)
    # if below, delete last two days of data (subject to change)
    usage = dbx.users_get_space_usage()
    used = usage.used
    allocated = usage.allocation.get_individual().allocated
    remaining_space = allocated - used
    print("remaining space: {}".format(str(remaining_space)))

    # delete any files older than 8 weeks
    min_date = datetime.datetime.now() - datetime.timedelta(days=56)

    # list files
    project_files = dbx.files_list_folder("", recursive=True).entries
    project_files = list(
        filter(lambda entry: type(entry) == dropbox.files.FileMetadata, project_files)
    )

    # list files older than min date --> name reference date
    date_pattern = "\d{4}-\d{2}-\d{2}"
    project_files_match = list(
        map(
            lambda entry: {
                "name": entry.name,
                "size": entry.size,
                "match": re.search(date_pattern, entry.name),
                "path": entry.path_lower,
            },
            project_files,
        )
    )
    project_files_match = list(
        filter(lambda entry: entry["match"] is not None, project_files_match)
    )
    project_files_refdate = list(
        map(
            lambda entry: {
                **entry,
                **{
                    "ref_date": entry["name"][
                        entry["match"].start() : entry["match"].end()
                    ]
                },
            },
            project_files_match,
        )
    )

    # list files older than min date --> name reference date
    project_files_refdate = list(
        filter(
            lambda entry: datetime.datetime.strptime(entry["ref_date"], "%Y-%m-%d")
            < min_date,
            project_files_refdate,
        )
    )

    if len(project_files_refdate) == 0:
        print("no files to delete")
        return None
    else:
        print(min_date)
        print("deleting {} files".format(str(len(project_files_refdate))))
        del_size = sum([entry["size"] for entry in project_files_refdate])
        print("freeing up {} bytes".format(str(del_size)))
        print([entry["name"] for entry in project_files_refdate])

        # delete old files
        for entry in project_files_refdate:
            path = entry["path"]
            dbx.files_delete(path)

        return None


if __name__ == "__main__":

    ensure_available_space()
