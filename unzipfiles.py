from zipfile import ZipFile
import os
from checkfiledate import checkfiledate


class ExtractFromZip:
    
    def __init__(self):
        self.file = None
        self.fileName = None
        self.lpath = None
        self.spath = None
        self.ending = ".zip"
    
    def extract(self, lpath: str, spath: str):
        """
        this method extracts the desired files from the DWD-zip-files. These files have a systematic file name,
        consisting of a name prefix ('produkt_klima_tag'), the period from which values are taken and the number
        of the weather station from which the values are taken. The txt-files saved are csv's with the values
        that finally are needed for building the DWD-raw-files for the data master-update.
        :lpath: the path for loading the zip files
        :spath: the path for saving the processed files
        :return: (saves the extracted txt-file)
        """
        self.lpath = lpath
        self.spath = spath
        
        # TODO: this needs to be refactored for use with buckets
        with os.scandir(self.lpath) as self.folder:
            for self.file in self.folder:
                if self.file.name.endswith(self.ending):
                    with ZipFile(self.file, 'r') as self.zipObj:
                        # Get a list of all archived file names from the zip
                        listOfFileNames = self.zipObj.namelist()
                        # Iterate over the file names
                        for self.fileName in listOfFileNames:
                            # Check filename starts with the relevant text
                            if self.fileName.startswith('produkt_klima_tag'):
                                # Extract a single file from zip
                                self.zipObj.extract(self.fileName, self.spath)
                                print("done: ", self.file.name)


if __name__ == "__main__":
    """
    second step of producing a raw file: unzip the downloaded archives and get the data
    out of each file.
    """
    
    # instance of the class for the extraction of the downloaded zip-archive
    unzip = ExtractFromZip()
    
    # these are the paths to the files to load...
    lpath = "DWD_Input"
    # ... and to the folder for the extracted files
    spath = "DWD_extracted"
    
    # before unzipping: check if the files really are new
    
    date = checkfiledate()
    date_new = date.get_first_file_creation_date(lpath)
    date_old = date.get_first_file_creation_date(spath)
    
    if date_new:
        if date.is_date_new(date_old, date_new):
            
            # doing the job
            unzip.extract(lpath, spath)
            
            # tell them that I am ready
            print("ready with extracting files")
        else:
                print("no new files")