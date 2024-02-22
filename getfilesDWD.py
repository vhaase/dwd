import os
from urllib.request import urlopen
# from urllib.request import urlopen
from zipfile import ZipFile

import requests
from bs4 import BeautifulSoup


class DWD:
    """
    in this class all functions are collected for fetching the DWD-data
    from their website. Also the preparation of the raw file for the data master update is part of this code.
    """
    
    def __init__(self):
        self.ending = None
        self.fileName = None
        self.file = None
        self.path: str = ""

    def listfilenames(self, url: str, ext: str = ''):
        """
        Objective is to download all files from DWD with weather observations. Those files are in the
        "recent" folder of the DWD-website.
        This method extracts the file-list from the given url for recents where the links to the
        files are represented by text-entries on a html-page.
        :param url: the html-site where the links to the zip-files are stored
        :param ext: this is the file-extension we are looking for
        :return: the whole list of filenames which can be found on the url's html-page
        """
        # the page represented as a textfile
        page = requests.get(url).text
        # the textfile representing the website now parsed to have the details for the download ready for processing
        soup = BeautifulSoup(page, 'html.parser')
        # this generates the list of filenames extracted from the url's html-page
        return [url + '/' + node.get('href') for node in soup.find_all('a') if node.get('href').endswith(ext)]
    
    def extract(self, ending: str, lpath: str, savepath: str):
        """
        this method extracts the desired  csv files (archived as .txt) from the DWD-zip-files. These files have a
        systematic file name, consisting of a name prefix ('produkt_klima_tag'), the period from which values are
        taken and the number of the weather station from which the values are taken. The txt-files saved are csv's
        with the values that finally are needed for building the DWD-raw-files for the data master-update.
        :param savepath:
        :param ending: file name of the file to extract
        :param lpath:  path to the file to extract
        :return: the extracted file will be saved to path "savepath"
        """
        self.path = lpath
        self.ending = ending
        
        # TODO: this needs to be refactored for using buckets instead of (local) disc
        with os.scandir(self.path) as self.folder:
            for self.file in self.folder:
                if self.file.name.endswith(ending):
                    with ZipFile(self.file, 'r') as self.zipObj:
                        # Get a list of all archived file names from the zip
                        listOfFileNames = self.zipObj.namelist()
                        # Iterate over the file names
                        for self.fileName in listOfFileNames:
                            # Check filename for which we are looking
                            if self.fileName.startswith('produkt_klima_tag'):
                                # Extract the desired file from zip and save it to temp_txt
                                self.zipObj.extract(self.fileName, savepath)
        print("done: ", self.fileName)


if __name__ == "__main__":
    
    """
    first step: download the files with observations from the DWD url. Their naming pattern is:
    tageswerte_KL_XXXXX_akt.zip, where XXXXX is the number of the weather-station. What we get is
    about  570 zip-files. From all of them we need a specific file which holds the data, name pattern of this file:
    produkt_klima_tag_YYYYYYYY_ZZZZZZZZ_XXXXX.txt. Where Y and Z render the period in which data is available in this
    file. Theh csv-data inside these Text-files is then put together (third step). The output is a parquet-file with
    entries of observation for each day of the period covered by the downloaded files.
    """

    # we get the data from this url:
    url = 'https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/daily/kl/recent/'
    
    # we are looking for zip-files, not the other stuff on this page
    ext = 'zip'
    
    # this is the folder where the downloaded files will be saved
    # TODO: replace by a bucket
    # pfad = "DWD_Input"
    pfad = r"C:\Users\VolkerHaase\PycharmProjects\dwd\DWD_Input"
    
    # create an instance of the DWD-class
    dwd = DWD()
    
    # here we have all filenames for the download in a list
    liste = dwd.listfilenames(url, ext)
    # counter for the number of files to be downloaded
    ll = len(liste)
    
    # looping through the list of files
    for file in liste:
        with urlopen(file) as fobj:
            try:
                content = fobj.read()
                # we download the file and store it in the desired place
                # TODO: needs to be using a bucket
                with open(os.sep.join([pfad, file.split("/")[-1]]), "w+b") as download:
                    download.write(bytearray(content))
                    print(download.name, " //", ll, "Files remaining")
                    ll -= 1
            except FileNotFoundError as e:
                print(f"File not found: {e} File: {file}")
            except Exception as e:
                print(f"Error occurred: {e} File: {file}")
