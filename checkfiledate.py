import os
import datetime


# TODO: not sure if that should be refactored to GCP-use, or if that is already covered by some DAG

class checkfiledate:
    """
    to avoid having data with the same date twice or even more often in the raw-file, with this class we check
    if data is already there with the same date. If yes, the processing of the data may come to q stop.
    just checking the first file in the folder.
    """
    
    def get_first_file_creation_date(self, folder_path: os.path) -> datetime:
        """
        telling us the time of creation of the first file in the folder path, we only take the first file because
        it is simple to do and in that location only a set of files from one session should be available
        Args:
            folder_path: the path to the folder where we look for the first file
        Returns: the datetime of file creation

        """
        # we need a list of files for the simple analysis to do
        file_list = [file for file in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, file))]
        
        # Make sure there's at least one file
        if file_list:
            first_file_path = os.path.join(folder_path, file_list[0])
            creation_timestamp = os.path.getctime(first_file_path)
            creation_date = datetime.datetime.fromtimestamp(creation_timestamp)
            return creation_date
        else:
            return None  # No files in the directory
    
    def is_date_new(self, dateold: datetime, datenew: datetime) -> bool:
        """
        we want to know, if the data to process in the folder/bucket is new or not
        Args:
            dateold: the date of the existing file
            datenew: the date of the file to process

        Returns: bool

        """
        if datenew and dateold:
            # just trying
            return datenew > dateold
        elif datenew and not dateold:
            # good if old file or no file in the folder
            return True


if __name__ == "__main__":
    
    date = checkfiledate()
    
    folder_path_new = "DWD_Input"  # new
    folder_path_old = "DWD_extracted"  # old
    
    creation_date = date.get_first_file_creation_date(folder_path_new)
    
    if creation_date:
        print(f"Creation date of the first file in {folder_path_new}: {creation_date}")
    else:
        print(f"No files found in {folder_path_new}")
    
    creation_date = date.get_first_file_creation_date(folder_path_old)
    
    if creation_date:
        print(f"Creation date of the first file in {folder_path_old}: {creation_date}")
    else:
        print(f"No files found in {folder_path_old}")
    
    date_new = date.get_first_file_creation_date(folder_path_new)
    
    date_old = date.get_first_file_creation_date(folder_path_old)
    
    if date.is_date_new(date_old, date_new):
        print("data is new")
    else:
        print("data is old")
