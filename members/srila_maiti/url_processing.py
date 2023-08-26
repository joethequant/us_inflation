# Importing libraries
import requests
import re
from bs4 import BeautifulSoup as bs
import pandas as pd
import numpy as np

"""
This class is for reading the data from url and store them in dataframe so
that it can be used later.
"""
class url_processing:
    """
    This class reads the data from a given url, does preprocessing and
    then convert the data to dataframe.
    """
    def __init__(self, url_str):
        """
        Parameters
        ----------
        url_str : string
        Description : url String

        Returns
        ----------
        None
        """
        self.url = url_str
        self.text_list = []
        self.header = []
        self.df_url_data = pd.DataFrame()

    def read_text_from_url(self):
        """
        This function reads the data from the url and then does the string
        processing, finally, it converts the long string in list of lists where
        each line in the file is an element of the list.
        Returns
        -------
        None.
        """
        req = requests.get(self.url)
        soup = bs(req.text)
        soup_str = str(soup)
        soup_str_list = (soup_str.replace('<html><body><p>','')
                                 .replace('</p></body></html>','')
                                 .replace("\r\n","\n")
                                 .replace("\t","|")
                                 .replace(" ","")
                                 .split("\n")
                        )
        self.text_list, self.header = soup_str_list[1:], soup_str_list[0].split("|")
        self.text_list = [text.split("|") for text in self.text_list]
        self.text_list = [text for text in self.text_list if len(text) != 1]

    def convert_text_to_df(self):
        """
        This function converts the list of list to a dataframe.
        Returns
        -------
        dataframe
        File data is converted to a dataframe and returned.
        """
        self.df_url_data = pd.DataFrame(self.text_list, columns = self.header)
        self.df_url_data['src'] = self.url.split("/")[-1]

        # There is no data value for footnotes
        if "footnote_codes" in self.df_url_data.columns:
            self.df_url_data.drop(columns=['footnote_codes'], inplace = True)
        
        if 'data' in self.url.split("/")[-1]:
            self.df_url_data['period'] = self.df_url_data['period'].apply(lambda x: 'S03' if x == 'M13' else x)
            self.df_url_data['agg_ind'] = self.df_url_data['period'].apply(lambda x: 0 if x[0] == 'M' else 1)
            self.df_url_data.loc[self.df_url_data['agg_ind'] == 1, 'fmt_dt'] = None
            self.df_url_data.loc[self.df_url_data['agg_ind'] == 0, 'fmt_dt'] = pd.DatetimeIndex(self.df_url_data[self.df_url_data.agg_ind == 0]['year'] + '-' + self.df_url_data[self.df_url_data.agg_ind == 0]['period'].apply(lambda x : x[1:]) + '-01')
            self.df_url_data.loc[:,'value'] = self.df_url_data['value'].astype(float)
            self.df_url_data = self.df_url_data[self.df_url_data.agg_ind == 0]
            self.df_url_data.loc[:, 'fmt_dt'] = pd.to_datetime(self.df_url_data['fmt_dt'])
            self.df_url_data.set_index('fmt_dt', inplace = True)
        
        return self.df_url_data

    def flatten_df(self, orig_df, extend_df, join_col, index_col):
        self.df_url_data = pd.merge(orig_df, extend_df, how = 'inner', on = join_col)
        self.df_url_data.set_index(index_col, inplace = True)
        #print(self.df_url_data.head())
        return self.df_url_data

    def __str__(self):
        return self.url

    def __repr__(self):
        return self.url

if __name__ == "__main__":

    url_list = ['https://download.bls.gov/pub/time.series/cu/cu.area',
                'https://download.bls.gov/pub/time.series/cu/cu.base',
                'https://download.bls.gov/pub/time.series/cu/cu.item',
                'https://download.bls.gov/pub/time.series/cu/cu.period',
                'https://download.bls.gov/pub/time.series/cu/cu.seasonal',
                'https://download.bls.gov/pub/time.series/cu/cu.periodicity',
                'https://download.bls.gov/pub/time.series/cu/cu.series',
                'https://download.bls.gov/pub/time.series/cu/cu.data.0.Current',
                'https://download.bls.gov/pub/time.series/cu/cu.data.1.AllItems',
                'https://download.bls.gov/pub/time.series/cu/cu.data.10.OtherWest',
                'https://download.bls.gov/pub/time.series/cu/cu.data.11.USFoodBeverage',
                'https://download.bls.gov/pub/time.series/cu/cu.data.12.USHousing',
                'https://download.bls.gov/pub/time.series/cu/cu.data.13.USApparel',
                'https://download.bls.gov/pub/time.series/cu/cu.data.14.USTransportation',
                'https://download.bls.gov/pub/time.series/cu/cu.data.15.USMedical',
                'https://download.bls.gov/pub/time.series/cu/cu.data.16.USRecreation',
                'https://download.bls.gov/pub/time.series/cu/cu.data.17.USEducationAndCommunication',
                'https://download.bls.gov/pub/time.series/cu/cu.data.18.USOtherGoodsAndServices',
                'https://download.bls.gov/pub/time.series/cu/cu.data.19.PopulationSize',
                'https://download.bls.gov/pub/time.series/cu/cu.data.2.Summaries',
                'https://download.bls.gov/pub/time.series/cu/cu.data.20.USCommoditiesServicesSpecial',
                'https://download.bls.gov/pub/time.series/cu/cu.data.3.AsizeNorthEast',
                'https://download.bls.gov/pub/time.series/cu/cu.data.4.AsizeNorthCentral',
                'https://download.bls.gov/pub/time.series/cu/cu.data.5.AsizeSouth',
                'https://download.bls.gov/pub/time.series/cu/cu.data.6.AsizeWest',
                'https://download.bls.gov/pub/time.series/cu/cu.data.7.OtherNorthEast',
                'https://download.bls.gov/pub/time.series/cu/cu.data.8.OtherNorthCentral',
                'https://download.bls.gov/pub/time.series/cu/cu.data.9.OtherSouth'
               ]

    url_data_df_list = []

    for url in url_list:
        url_proc = url_processing(url)
        print("Processing", url_proc)
        url_proc.read_text_from_url()
        url_data_df_list.append(url_proc.convert_text_to_df())
        print("****************")
