# -*- coding: utf-8 -*-
"""

## IMPORTANT: FOLDER STRUCTURE ##
Below mentioned folder structure must be followed for getting the results from this script:
01. Script must be placed into a folder known as main folder
02. Source files (3 lists) must be placed into the Source folder under main folder
03. a folder named as IntermediateFiles must be created under main folder with below mentioned sub-folders
    A. Source
    B. Preprocessed
    C. IntermediateFiles
        i.  DDM
        ii. PDM

## IMPORTANT: EXECUTION ORDER ##
Below scripts must be exected in the mentioned order:
01. 01_RecordLinkageDDM.py
02. 02_RecordLinkagePDM.py
03. 03_RecordLinkageDDMScore.py
04. 04_RecordLinkagePDMScore.py

This script generates record based score in case of a match between customer list and negative/positive lists. It performs below mentioned steps:
01. Reads data from the files generated after DDM completion
02. generates record based score corresponding to all the DDM records
03. Generates files under DDM folder present under IntermediateFiles folder

"""

# Load required packages
import os
import pandas as pd
import recordlinkage
from datetime import datetime
import numpy as np
from difflib import SequenceMatcher
import math


def extractSource(dir, files):
    '''
    Function to extract Data from files in a specific format
    '''
    t = {"FIRST_NAME": object, "LAST_NAME": object, "DOB": object, "STREET": object,"ZIP": object, "CITY": object, "HNRNEW": object}
    files = datetime.now().strftime("%Y%m%d") + "_" + files
    filename = dir + files
    df = pd.read_csv(filename, index_col="ID", na_values="0000-00-00", dtype=t)
    return df

def similar(a, b):
    '''
    Function to generate similarity score between two strings (a, b)
    '''
    return SequenceMatcher(None, a, b).ratio()

def scorePOS(rec, df_cust, df_pos):
    '''
    Function to assign weights to each column, generate similarity score corresponding to each value in the matched rows and come up with overall match score for the matched records between customer and positive lists
    '''
    W = {"FIRST_NAME": 19, "LAST_NAME": 25, "DOB": 28, "STREET": 11,"ZIP": 6, "CITY": 8, "HNRNEW": 3}
    cust_rec = df_cust.loc[rec['ID_CUST']]
    pos_rec = df_pos.loc[rec['ID_POS']]
    df = pd.DataFrame()
    null_col = (cust_rec[cust_rec.isna()].index | pos_rec[pos_rec.isna()].index).tolist()
    R = 0.0
    for k in W.keys():
        if k in null_col:
            R += W[k]
            W[k] = 0
    D = {}
    for k in W.keys():
        W[k] = W[k] * 100 / (100 - R)
        if k in null_col:
            D[k] = 0.0
        else:
            if (k  == 'DOB'):
                if (similar(cust_rec[k], pos_rec[k]) < 1):
                    D[k] = 0
                else:
                    D[k] = 1
            else:
                D[k] = similar(cust_rec[k], pos_rec[k])
    W = pd.Series(W, name = 'W')   
    D = pd.Series(D, name = 'D')
    df = pd.concat([D, W], axis=1)
    df['S'] = df['D'] * df['W']
    return df['S'].sum()

def scoreNEG(rec, df_cust, df_neg):
    '''
    Function to assign weights to each column, generate similarity score corresponding to each value between matched rows and come up with overall match score for the matched records between customer and negative lists
    '''
    W = {"FIRST_NAME": 19, "LAST_NAME": 25, "DOB": 28, "STREET": 11,"ZIP": 6, "CITY": 8, "HNRNEW": 3}
    cust_rec = df_cust.loc[rec['ID_CUST']]
    neg_rec = df_neg.loc[rec['ID_NEG']]
    df = pd.DataFrame()
    null_col = (cust_rec[cust_rec.isna()].index | neg_rec[neg_rec.isna()].index).tolist()
    R = 0.0
    for k in W.keys():
        if k in null_col:
            R += W[k]
            W[k] = 0
    D = {}
    for k in W.keys():
        W[k] = W[k] * 100 / (100 - R)
        if k in null_col:
            D[k] = 0.0
        else:
            if (k  == 'DOB'):
                if (similar(cust_rec[k], neg_rec[k]) < 1):
                    D[k] = 0
                else:
                    D[k] = 1
            else:
                D[k] = similar(cust_rec[k], neg_rec[k])
    W = pd.Series(W, name = 'W')   
    D = pd.Series(D, name = 'D')
    df = pd.concat([D, W], axis=1)
    df['S'] = df['D'] * df['W']
    return df['S'].sum()

def MatchScore(ddm, cust, pos, neg):
    '''
    Function to identify the matched rows and call the appropriate function to get row based score
    '''
    d = {}
    for i in range(len(ddm)):
        rec = ddm.iloc[i]
        if pd.isnull(rec['ID_NEG']):
            d[i] = scorePOS(rec, cust, pos)
        elif pd.isnull(rec['ID_POS']):
            d[i] = scoreNEG(rec, cust, neg)
        else:
            d[i] = 0
    ddm['NEW_SCORE'] = ddm.index.to_series().map(d)
    return ddm

def MatchedFiles(dir, file, df):
    '''
    Function to create intermediate files post a specific operation like Data Preprocessing without index values
    '''
    originalFiles = ["00_List_Customer_Monitoring.csv", "01a_List_Negative.csv", "01b_List_Positive.csv"]
    if file in originalFiles:
        file = file
    else:
        file = datetime.now().strftime("%Y%m%d") + "_" + file
    filename = dir + file
    df.to_csv(filename, index=False)

# Main function - starting point of the script
if __name__ == "__main__":
    print("Data Load started: " + str(datetime.now()))
    cwd = os.getcwd()
    intFileDir = cwd + r"\\IntermediateFiles\\Preprocessed\\"
    print("CUSTOMER MONITORING LIST")
    custFile = r"PP_00_List_Customer_Monitoring.csv"
    df_cust = extractSource(intFileDir, custFile)
    print("NEGATIVE LIST")
    negFile = r"PP_01a_List_Negative.csv"
    df_neg = extractSource(intFileDir, negFile)
    print("POSITIVE LIST")
    posFile = r"PP_01b_List_Positive.csv"
    df_pos = extractSource(intFileDir, posFile)
    print("Data Load Completed!!! " + str(datetime.now()))
    intFileDir = cwd + r"\\IntermediateFiles\\DDM\\"
    ddmFile = intFileDir + datetime.now().strftime("%Y%m%d") + '_' + r'DDM.csv'
    df_ddm = pd.read_csv(ddmFile)
    df_ddm1 = df_ddm.copy()
    df_ddm1 = MatchScore(df_ddm1, df_cust, df_pos, df_neg)
    ddmFile1 = r'DDM1.csv'
    MatchedFiles(intFileDir, ddmFile1, df_ddm1)