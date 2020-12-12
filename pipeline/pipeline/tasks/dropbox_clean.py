# from pipeline.config import DROPBOX_TOKEN
from luigi.contrib.dropbox import DropboxTarget
from pipeline.config import DROPBOX_TOKEN
import os

DROPBOX_TOKEN = os.getenv("SWB_DROPBOX_TOKEN") # TODO get from pipeline.config

# duplicate of a func in dropbox.py
def dropbox_target(path):
    return DropboxTarget(path, DROPBOX_TOKEN) 


# ? ==============================
# ? dropbox_clear_space
# ? Additions re: Dropbox clear space
def ensure_available_space(min_space):

    dbx = dropbox.Dropbox(DROPBOX_TOKEN) 

    # ? print usage
    usage = dbx.users.SpaceUsage()
    print(usage)

    # ? list files
    for entry in dbx.files_list_folder("").entries:
        print(entry.name)
        print(dbx.files_get_metadata(entry.name))

    # ? delete file
    # path = ""
    # dbx.files_delete(path)

    return None


if __name__ == "__main__":

    ensure_available_space()
