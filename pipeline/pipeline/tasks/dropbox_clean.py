from pipeline.config import DROPBOX_TOKEN
import dropbox

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
