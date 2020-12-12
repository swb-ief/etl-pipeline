
from pipeline.config import DROPBOX_TOKEN
import dropbox 

#? ==============================
#? dropbox_clear_space
#? Additions re: Dropbox clear space
def available_space():

    dbx = dropbox.Dropbox(DROPBOX_TOKEN)

    #? list files
    for entry in dbx.files_list_folder('').entries:
        print(entry.name)
        print(dbx.files_get_metadata(entry.name))


    return None


if __name__ == "__main__":

    available_space()