import pathlib
import typing
import glob2 as glob
import polars as pl


class buildraw:
    """
    a class with functionality to build the raw file which serves as basis for the data master.
    All csv-files in a folder will be combined to one large CSV-file.
    """
    
    def __init__(self):
        
        self.df_temp = pl.DataFrame()
        self.df_load = pl.DataFrame()
        self.folder: str = None
        self.fname: str = None
        self.srcpath: str = None
        self.ending: str = None
        self.files = None
        self.filename = None
    
    def collectfromcsv(self, srcpath: str) -> pl.dataframe:
        """
        This method opens all csv-files with data for the daily observations. Unwanted
        columns will not be processed.
        These details are written to a df, and from the desired columns the
        mean value is calculated.
        :param srcpath: path to the csv.files extracted from DWD-zips
        :return: df with all data from the files fond on the html-page of DWD
        """
        # TODO: refactor for use with buckets
        # declarations
        self.ending = "txt"  # the ending of the csv-files to import
        self.srcpath = srcpath  # where the csv-files for import are located
        
        # walk through the folder
        self.path = str(self.srcpath) + '/*.txt'
        self.files = glob.glob(self.path)
        if self.files.__len__() > 0:
            for self.filename in self.files:
                self.df_load = pl.read_csv(self.filename, infer_schema_length=0, separator=";")
                # add the new data frame to an intermediate df
                try:
                    if self.df_temp.height > 0:
                        self.df_temp.extend(self.df_load)
                    else:
                        self.df_temp = self.df_load
                except Exception as e:
                    print(f"Error: {e}")
                else:
                    print(f"ok {self.filename}")
            
            print("ready with importing raw-data")
            return self.df_temp
        else:
            # return an empty df if nothing found to collect
            return self.df_temp.clear(0)
    
    def makedatatypes(self, df):
        """
        Set the desired data types for each column. Because the same columns have different
        data types for the different weather stations (changing between int and float)
        the schema is ignored when loading the source tables. And for simplicity all columns apart date are set to
        float32. Anyway the values in each column will be the average over all weather stations on a daily basis,
        so that the resulting table will anyway show float values.
        :param df: the df with columns to change
        :return: the df with data types adjusted
        """
       
        self.df = df
        
        # take away the leading whitespaces. These result from the import-process. The  columns are imported as string
        # and then made float. Every column finally shows average values, so float fits to that
        self.df = self.df.select(pl.col(pl.Utf8).str.strip())
        
        # replace dummy values ("-999") with zero
        self.df = self.df.select(pl.col(pl.Utf8).str.replace_all(r"-999", "0"))
        
        # date column
        self.df = self.df.with_columns(
            pl.col('datum').str.strptime(pl.Date, format="%Y%m%d", strict=False).alias('datum'))
        
        # all other columns float
        self.df = self.df.with_columns(
            [pl.col(df_import.columns[0])] + [pl.col(col_name).cast(pl.Float32) for col_name in df_import.columns[1:]]
        )
        return self.df
    
    def dfmakeover(self, df: pl.dataframe) -> pl.dataframe:
        """
        some manipulations with the imported data to make them easier to process in the next steps
        :param df:the data frame to transform
        :return: the data frame after transformation:
        """
        columns_to_drop = ['STATIONS_ID', 'QN_3', 'QN_4', 'RSKF', 'SHK_TAG', ' VPM', 'eor']
        df = df.drop(columns_to_drop)
        
        # we want to use the column names according to our conventions
        df = df.rename(
            {'MESS_DATUM': 'datum', '  FX': 'cli_dwd_windspitze', '  FM': 'cli_dwd_wind',
             ' RSK': 'cli_dwd_niederschlag',
             ' SDK': 'cli_dwd_sonne', '  NM': 'cli_dwd_bedeckung', '  PM': 'cli_dwd_luftdruck',
             ' TMK': 'cli_dwd_temperatur',
             ' UPM': 'cli_dwd_feuchte', ' TXK': 'cli_dwd_templuftmax', ' TNK': 'cli_dwd_templuftmin',
             ' TGK': 'cli_dwd_tempboden'})
        
        return df
    
    def grpdays(self, df: pl.dataframe) -> pl.dataframe:
        """
        the very basic step to have data consolidated on a daily basis
        Args:
            df: the dataframe to process
        Returns: the data frame processd
        """
        self.df = df
        return self.df.groupby("datum", maintain_order=True).mean()


if __name__ == "__main__":
    
    # TODO: write result into GCP-Bucket
    
    """
    third step: from all csv (i.e. .txt) files we now put together the
    data into one raw-file (currently also csv). For the data master
    update this raw data needs to be consolidated: for each date we must build
    the average values. Currently this should be part of the preprocessing, or
    even the aggregation part of the update.
    """
    
    # the paths for the files
    importpath = "DWD_extracted"
    savepath = "DWD_final"
    
    # instance of the class for the import of DWD-Data
    load = buildraw()
    
    # we call the method to collect the raw-data from the tables in the zip-archives
    df_import = load.collectfromcsv(importpath)
    
    if df_import.height > 0:
        # prepare the df as needed
        df_import = load.dfmakeover(df_import)
        df_import = load.makedatatypes(df_import)
        df_import = load.grpdays(df_import)
        
        # try to load an existing master-raw-file with the name DWD_raw.csv
        try:
            df_raw = pl.read_parquet(pathlib.Path(savepath, "DWD_raw.parquet"))
        except FileNotFoundError as f:
            # no file present: make the imported data frame the new master file
            df_raw = df_import
            df_raw.write_parquet(pathlib.Path(savepath, "DWD_raw.parquet"))
        except Exception as e:
            print(f"Error: {e} while trying to load/save the dwd raw-file")
        else:
            # finally put the two df together
            df_raw.extend(df_import)
            try:
                df_raw.write_parquet(pathlib.Path(savepath, "DWD_raw.parquet"))
            except PermissionError as p:
                print(
                    f"I cannot access the file: {p}. I will save my work under this name instead: DWD_save_me.parquet")
                df_raw.write_parquet(pathlib.Path(savepath, "DWD_raw.parquet"))
        print("ready with DWD consolidated parquet-file preparation")
    
    else:
        print("no data in \"extracted\"-Folder")
