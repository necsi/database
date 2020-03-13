#
# Author: Michael Buchel
# Company: MIM Technology Group Inc.
# Reason: Moved commonly used functions to a seperate file.
#
import csv
import codecs
import requests
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from contextlib import closing

def list_github_files(url, ext_to_ignore=[]):
    """
    Function written originally by bobbywlindsey

    Get a list of files in a folder inside a GitHub repository.

    :param url: str
    :param ext_to_ignore: list
    :return: list
    """
    response = requests.get(list_of_csvs)
    soup = BeautifulSoup(response.text, 'html.parser')
    table = soup.find_all('table', {'class': 'files js-navigation-container js-active-navigation-container'})[0]
    rows = table.find_all('tr')
    file_names = []
    for row in rows:
        content_column = row.find_all('td', {'class': 'content'})
        if content_column:
            content_column_link_tag = content_column[0].find('a')
            if content_column_link_tag:
                file_names.append(content_column_link_tag.text)
    if ext_to_ignore:
        filtered_file_names = [file_name for file_name in file_names if not file_name.endswith(tuple(extensions_to_ignore))]
    return filtered_file_names


def download_csv_to_dataframe(url):
    """
    Function written originally by bobbywlindsey

    Download a CSV file and return a Pandas DataFrame.

    :param url: str
    :return: pandas.DataFrame
    """
    with closing(requests.get(url, stream=True)) as r:
        reader = csv.reader(codecs.iterdecode(r.iter_lines(), 'utf-8'), delimiter=',', quotechar='"')
        data = [row for row in reader]
        header_row = data[0]
        data = data[1:]
        df = pd.DataFrame(data = data, index=np.arange(1, len(data)+1), columns=header_row)
        return df
