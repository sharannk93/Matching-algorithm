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

This script performs determistics data matching between customer list and negative/positive list. It performs below mentioned steps:
01. Reads data from the source files - 00_List_Customer_Monitoring.csv, 01a_List_Negative.csv and 01b_List_Positive.csv, placed under Source folder
02. Performs data preprocessing and cleaning on the loaded data
03. Generates the files under Preprocessed folder present under IntermediateFiles folder
03. Matches data using different determistics record linkage rules
04. Generates files under DDM folder present under IntermediateFiles folder

"""

# Load required packages
import os
import pandas as pd
import numpy as np
from datetime import datetime
import unicodedata


def extractSource(dir, files):
    '''
    Function to extract Data from files in a specific format
    '''
    t = {"FIRST_NAME": object, "LAST_NAME": object, "DOB": object, "STREET": object, "HNR": object, "HNRADD": object, "ZIP": object, "CITY": object}
    filename = dir + files
    df = pd.read_csv(filename, index_col="ID", na_values="0000-00-00", parse_dates=[3], dtype=t)
    return df


def caseConvertion(df):
    '''
    Function to convert string values present into columns to lowercase letters
    '''
    df = df.applymap(lambda s:s.lower() if type(s) == str else s)
    return df


def stripList(df):
    '''
    Function to trim the string values present into columns
    '''
    df = df.applymap(lambda s:s.strip() if type(s) == str else s)
    return df


def removeSpecialChar(s):
    '''
    Function to replace special symbols present with a space
    '''
    spec_chars = ["!", '"', "#", "%", "&", "'", "(", ")", "*", "+", ",", "-", ".", "/", ":", ";", "<", "=", ">", "?", "@", "[", "\\", "]", "^", "_", "`", "{", "|", "}", "~", "–"]
    for char in spec_chars:
        s = s.replace(char, " ")
        s = " ".join(s.split())
    return s


def removeSpecial(df):
    '''
    Function to replace special characters from all the values present into different columns
    '''
    df = df.applymap(lambda s:removeSpecialChar(s) if type(s) == str else s)
    return df


def removeTitleName(s):
    '''
    Function to remove titles and honorifics from a series
    '''
    titles = ["ms ", "mr ", "mrs ", "miss ", "master ", "professor ", "dr ", "herr ", "frau ", "prof "]
    for title in titles:
        s = s.str.replace(title, "").str.strip()
    return s


def removeTitle(df):
    '''
    Function to remove titles and honorifics from first name and last name
    '''
    df["FIRST_NAME"] = removeTitleName(df["FIRST_NAME"])
    df["LAST_NAME"] = removeTitleName(df["LAST_NAME"])
    return df


def replaceUmlaut(df):
    '''
    Function to replace umlauts (ä --> ae, ö --> oe, ü --> ue, ß --> ss)
    '''
    df = df.applymap(lambda s:s.replace('ä','ae') if type(s) == str else s)
    df = df.applymap(lambda s:s.replace('ö','oe') if type(s) == str else s)
    df = df.applymap(lambda s:s.replace('ü','ue') if type(s) == str else s)
    df = df.applymap(lambda s:s.replace('ß','ss') if type(s) == str else s)
    return df


def removeAccentedChars(s):
    '''
    Function to replace special characters like accents to ASCII characters from a series 
    '''
    s = unicodedata.normalize('NFKD', s).encode('ascii', 'ignore').decode('utf-8', 'ignore')
    return s


def removeAccented(df):
    '''
    Function to replace special characters like accents to ASCII characters from the dataframe
    '''
    df = df.applymap(lambda s:removeAccentedChars(s) if type(s) == str else s)
    return df


def formatZip(df):
    '''
    Function to standardize ZIP to a five characters format by appending leading zeroes.
    '''
    df['ZIP'] = df['ZIP'].str.zfill(5)
    return df


def formatCity(df):
    '''
    Function to correct common spelling mistakes in CITY
    '''
    df['CITY'] = df['CITY'].str.replace('mainz a r', 'mainz')
    df['CITY'] = df['CITY'].str.replace('frankfurt a m', 'frankfrut am main')
    df['CITY'] = df['CITY'].str.replace('frankfurt am', 'frankfrut am main')
    df['CITY'] = df['CITY'].str.replace('frankfurt a main', 'frankfrut am main')
    df['CITY'] = df['CITY'].str.replace('frankfurt m', 'frankfrut a main')
    df.loc[(df['CITY'] == 'frankfurt') & (df['ZIP'].str.startswith('1')), 'CITY'] = 'frankfurt oder'
    df.loc[(df['CITY'] == 'frankfurt') & (df['ZIP'].str.startswith('6')), 'CITY'] = 'frankfurt am main'
    return df


def extractHNR(df):
    '''
    Function to extract HNR from STREET if it contains any number
    '''
    pattern1 = r'([\d]+)'
    pattern2 = r'([\d]+[\w])'
    pattern3 = r'([\d]+\s[\w])'
    pattern4 = r'([\d]+\s[\d]+\s[\w]\s[\d]+)'
    df["PAT3"] = df["STREET"].str.extract(pattern3).replace(' ', '')
    df["PAT2"] = df["STREET"].str.extract(pattern2).replace(' ', '')
    df["PAT1"] = df["STREET"].str.extract(pattern1).replace(' ', '')
    df['HNRNEW'] = df['PAT3'].fillna(df["PAT2"].fillna(df["PAT1"]))
    df['HNRNEW'] = df['HNRNEW'].str.replace(' ', '')
    df["STREET"] = df["STREET"].str.replace(pattern4, '').str.strip()
    df["STREET"] = df["STREET"].str.replace(pattern3, '').str.strip()
    df["STREET"] = df["STREET"].str.replace(pattern2, '').str.strip()
    df["STREET"] = df["STREET"].str.replace(pattern1, '').str.strip()
    df = df.drop(columns=['PAT3'], axis = 1)
    df = df.drop(columns=['PAT2'], axis = 1)
    df = df.drop(columns=['PAT1'], axis = 1)
    return df


def joinColumns(df):
    '''
    Function to combine HNR and HNRADD to form a new column as HNRNEW and dropping the HNR and HNRADD columns from the dataframe
    '''
    df["HNR"] = df["HNR"].astype(str)
    df["HNRADD"] = df["HNRADD"].astype(str)
    df["HNR"] = df["HNR"].str.replace(' ', '')
    df["HNR"] = df["HNR"].str.replace('nan', '')
    df["HNRADD"] = df["HNRADD"].str.replace(' ', '')
    df["HNRADD"] = df["HNRADD"].str.replace('nan', '')
    df["HNRNEW"] = df['HNR'].str.strip().fillna('') + df['HNRADD'].str.strip().fillna('')
    df["HNRNEW"] = df["HNRNEW"].str.strip().replace(r'^\s*$', np.nan, regex=True)
    df = df.drop(columns=['HNR'], axis = 1)
    df = df.drop(columns=['HNRADD'], axis = 1)
    return df


def formatStreet(df):
    '''
    Function to standardise street name (' str', ' strsse', ' srasse', 'str$', 'strsse$', 'srasse$' --> 'strasse')
    '''
    df['STREET'] = df['STREET'].str.replace(' str','strasse')
    df['STREET'] = df['STREET'].str.replace(' strsse','strasse')
    df['STREET'] = df['STREET'].str.replace(' srasse','strasse')
    df['STREET'] = df['STREET'].str.replace('str$','strasse')
    df['STREET'] = df['STREET'].str.replace('strsse$','strasse')
    df['STREET'] = df['STREET'].str.replace('srasse$','strasse')
    return df


def dataPreprocessing(df):
    '''
    Function to perfrom data preprocessing and cleaning on customer list as per below mentioned order:
    01. Convert case to lowercase
    02. Remove the whitespace from the string columns
    03. Remove special symbols by a single space
    04. Remove titles and honorifics from the name
    05. Replace german umlaut to english equivalents
    06. Replace special characters with ASCII characters
    07. Standardized the Zip
    08. Standardized the city name
    09. Extract HNR from STREET name
    10. Formation of HNRNEW
    11. Standardized the Street name
    '''
    df = caseConvertion(df)
    df = stripList(df)
    df = removeSpecial(df)
    df = removeTitle(df)
    df = replaceUmlaut(df)
    df = removeAccented(df)
    df = formatZip(df)
    df = formatCity(df)
    df = extractHNR(df)
    df = joinColumns(df)
    df = formatStreet(df)
    return df

def dataPreprocessing1(df):
    '''
    Function to perfrom data preprocessing and cleaning on positive and negative lists as per below mentioned order:
    01. Convert case to lowercase
    02. Remove the whitespace from the string columns
    03. Remove special symbols by a single space
    04. Remove titles and honorifics from the name
    05. Replace german umlaut to english equivalents
    06. Replace special characters with ASCII characters
    07. Standardized the Zip
    08. Standardized the city name
    09. Extract HNR from STREET name
    10. Formation of NEWHNR
    11. Standardized the Street name
    12. Drop duplicate rows
    '''
    df = caseConvertion(df)
    df = stripList(df)
    df = removeSpecial(df)
    df = removeTitle(df)
    df = replaceUmlaut(df)
    df = removeAccented(df)
    df = formatZip(df)
    df = formatCity(df)
    df = extractHNR(df)
    df = joinColumns(df)
    df = formatStreet(df)
    df.drop_duplicates(inplace=True)
    return df


def IntermediateFiles(dir, file, df):
    '''
    Function to create intermediate files post a specific operation like Data Preprocessing with index values
    '''
    originalFiles = ["00_List_Customer_Monitoring.csv", "01a_List_Negative.csv", "01b_List_Positive.csv"]
    if file in originalFiles:
        file = file
    else:
        file = datetime.now().strftime("%Y%m%d") + "_" + file
    filename = dir + file
    df.to_csv(filename)


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


def matchedIndex(nmatch, pmatch, rule):
    '''
    Function to append customers matched with negative list to the customers matched with positive list
    '''
    match = nmatch.append(pmatch, ignore_index=True, sort=False)
    match = match[["ID_CUST", "ID_NEG", "ID_POS", "MATCH_CRITERIA", "MATCH_SCORE"]]
    match.sort_values(by=["ID_CUST"], inplace=True)
    match = match.reset_index()
    match = match.drop("index", axis = 1)
    return match


def colMatchDDMPOS(cust, pos, condition, i):
    '''
    Function to match customer with the positive list based on a condition
    '''
    print("Start of Rule" + str(i) + ": [" + ', '.join(condition[:-1]) + "]")
    pos_match = pd.merge(cust, pos, how="inner", on=condition[:-1], suffixes=["_CUST", "_POS"])
    matchCriteria = "DDM RULE" + str(i) + ": " + ', '.join(condition[:-1])
    pos_match["MATCH_CRITERIA"] = matchCriteria
    pos_match["MATCH_SCORE"] = float(condition[-1])
    matched_index = pos_match[["ID_CUST", "ID_POS", "MATCH_CRITERIA", "MATCH_SCORE"]]
    matched_index.sort_values(by=["ID_CUST"], inplace=True)
    matched_index = matched_index.reset_index()
    matched_index = matched_index.drop("index", axis = 1)
    cust = cust[~cust["ID"].isin(matched_index["ID_CUST"])]
    print("End of Rule" + str(i) +": [" + ', '.join(condition[:-1]) + "]")
    return matched_index, cust


def colMatchDDMNEG(cust, neg, condition, i):
    '''
    Function to match customer with the negative list based on a condition
    '''
    print("Start of Rule" + str(i) + ": [" + ', '.join(condition[:-1]) + "]")
    neg_match = pd.merge(cust, neg, how="inner", on=condition[:-1], suffixes=["_CUST", "_NEG"])
    matchCriteria = "DDM RULE " + str(i) + ": " + ', '.join(condition[:-1])
    neg_match["MATCH_CRITERIA"] = matchCriteria
    neg_match["MATCH_SCORE"] = float(condition[-1])
    matched_index = neg_match[["ID_CUST", "ID_NEG", "MATCH_CRITERIA", "MATCH_SCORE"]]
    matched_index.sort_values(by=["ID_CUST"], inplace=True)
    matched_index = matched_index.reset_index()
    matched_index = matched_index.drop("index", axis = 1)
    cust = cust[~cust["ID"].isin(matched_index["ID_CUST"])]
    print("End of Rule" + str(i) +": [" + ', '.join(condition[:-1]) + "]")
    return matched_index, cust


def DDM(cust_df, neg_df, pos_df):
    '''
    Function to iteratively perform DDM for all the defined rules
    '''
    print("Start DDM")
    # Replace 0000-00-00 with 1900-00-00 in customer list to avoid invalid matches 
    cust_df["DOB"] = cust_df["DOB"].fillna('1900-00-00')
    cust_df = cust_df.fillna('-99999')
    # Replace 0000-00-00 with 1800-00-00 in negative list to avoid invalid matches
    neg_df["DOB"] = neg_df["DOB"].fillna('1800-00-00')
    neg_df = neg_df.fillna('-88888')
    # Replace 0000-00-00 with 1700-00-00 in negative list to avoid invalid matches
    pos_df["DOB"] = pos_df["DOB"].fillna('1700-00-00')
    pos_df = pos_df.fillna('-77777')
    cust_pos_df = cust_df.copy()
    cust_neg_df = cust_df.copy()
    cust_pos_df['FN'] = cust_pos_df['FIRST_NAME']
    cust_pos_df['LN'] = cust_pos_df['LAST_NAME']
    cust_neg_df['FN'] = cust_neg_df['FIRST_NAME']
    cust_neg_df['LN'] = cust_neg_df['LAST_NAME']
    neg_df['FN'] = neg_df['LAST_NAME']
    neg_df['LN'] = neg_df['FIRST_NAME']
    pos_df['FN'] = pos_df['LAST_NAME']
    pos_df['LN'] = pos_df['FIRST_NAME']
    cust_df = cust_df.reset_index()
    neg_df = neg_df.reset_index()
    pos_df = pos_df.reset_index()
    cust_pos_df = cust_pos_df.reset_index()
    cust_neg_df = cust_neg_df.reset_index()
    # Rules or conditions to perform DDM along with rule based matching score
    condition1 = ['FIRST_NAME', 'LAST_NAME', 'DOB', 'ZIP', 'CITY', 'STREET', 'HNRNEW', 100]
    condition2 = ['FIRST_NAME', 'LAST_NAME', 'DOB', 'ZIP', 'STREET', 'HNRNEW', 99.4]
    condition3 = ['FIRST_NAME', 'LAST_NAME', 'DOB', 'CITY', 'STREET', 'HNRNEW', 97.9]
    condition4 = ['FIRST_NAME', 'LAST_NAME', 'DOB', 97.2]
    condition5 = ['LAST_NAME', 'DOB', 'ZIP', 'CITY', 'STREET', 'HNRNEW', 95.6]
    condition6 = ['FIRST_NAME', 'DOB', 'ZIP', 'CITY', 'STREET', 'HNRNEW', 91]
    condition7 = ['FIRST_NAME', 'LAST_NAME', 'ZIP', 'CITY', 'STREET', 'HNRNEW', 90.2]
    condition8 = ['FIRST_NAME', 'LAST_NAME', 'ZIP', 'STREET', 'HNRNEW', 89]
    condition9 = ['FIRST_NAME', 'LAST_NAME', 'CITY', 'STREET', 'HNRNEW', 87]
    condition10 = ['FIRST_NAME', 'LAST_NAME', 'ZIP', 'CITY', 'STREET', 84]
    condition11 = ['FN', 'LN', 'DOB', 83.5]
    condition12 = ['FN', 'LN', 'ZIP', 'CITY', 'STREET', 'HNRNEW', 83]
    condition13 = ['LAST_NAME', 'DOB', 'ZIP', 81.6]
    condition14 = ['DOB', 'ZIP', 'CITY', 'STREET', 'HNRNEW', 78]
    condition15 = ['FIRST_NAME', 'DOB', 'ZIP', 76]
    matchConditions = [condition1, condition2, condition3, condition4, condition5, condition6, condition7, condition8, condition9, condition10, condition11, condition12, condition13, condition14, condition15]
    cwd = os.getcwd()
    intFileDir = cwd + r"\\IntermediateFiles\\DDM\\"
    i = 1
    custFile = r"00_List_Customer_Monitoring.csv"
    matched_idx = pd.DataFrame()
    for matchCondition in matchConditions:
        FileNamePOS = "DDM_POS_Rule" + str(i) + ".csv"
        FileNameNEG = "DDM_NEG_Rule" + str(i) + ".csv"
        custFilePOSPostMatch = "DDM_POS_Rule" + str(i) + "_" + custFile
        custFileNEGPostMatch = "DDM_NEG_Rule" + str(i) + "_" + custFile
        idxPOS, cust_pos_df = colMatchDDMPOS(cust_pos_df, pos_df, matchCondition, i)
        idxNEG, cust_neg_df = colMatchDDMNEG(cust_neg_df, neg_df, matchCondition, i)
        MatchedFiles(intFileDir, FileNamePOS, idxPOS)
        MatchedFiles(intFileDir, custFilePOSPostMatch, cust_pos_df)
        MatchedFiles(intFileDir, FileNameNEG, idxNEG)
        MatchedFiles(intFileDir, custFileNEGPostMatch, cust_neg_df)
        matched_idx = matched_idx.append(matchedIndex(idxNEG, idxPOS, matchCondition), ignore_index=True, sort=False)
        matched_idx.sort_values(by=["ID_CUST"], inplace=True)
        i += 1
    cust_df = cust_df[~cust_df["ID"].isin(matched_idx["ID_CUST"])]
    print("End of DDM")
    return matched_idx, cust_df


# Main function - starting point of the script
if __name__ == "__main__":
    print("Data Load started: " + str(datetime.now()))
    cwd = os.getcwd()
    srcFolder = cwd + r"\\Source\\"
    print("CUSTOMER MONITORING LIST")
    custFile = r"00_List_Customer_Monitoring.csv"
    df_cust = extractSource(srcFolder, custFile)
    print("NEGATIVE LIST")
    negFile = r"01a_List_Negative.csv"
    df_neg = extractSource(srcFolder, negFile)
    print("POSITIVE LIST")
    posFile = r"01b_List_Positive.csv"
    df_pos = extractSource(srcFolder, posFile)
    print("Data Load Completed: " + str(datetime.now()))
    print(len(df_cust), len(df_neg), len(df_pos))
    print("Data Preprocessing Started: " + str(datetime.now()))
    print("CUSTOMER MONITORING LIST")
    df_cust = dataPreprocessing(df_cust)
    print("NEGATIVE LIST")
    df_neg = dataPreprocessing1(df_neg)
    print("POSITIVE LIST")
    df_pos = dataPreprocessing1(df_pos)
    print("Data Pre-processing Completed!!! " + str(datetime.now()))
    print(len(df_cust), len(df_neg), len(df_pos))
    print("Data load of preprocessed file started: " + str(datetime.now()))
    intFileDir = cwd + r"\\IntermediateFiles\\Preprocessed\\"
    ppCustFile = "PP_" + custFile
    IntermediateFiles(intFileDir, ppCustFile, df_cust)
    ppNegFile = "PP_" + negFile
    IntermediateFiles(intFileDir, ppNegFile, df_neg)
    ppPosFile = "PP_" + posFile
    IntermediateFiles(intFileDir, ppPosFile, df_pos)
    print("Data load of preprocessed file completed!!! " + str(datetime.now()))
    print("Determistics Data Match started: " + str(datetime.now()))
    index_df, df_cust = DDM(df_cust, df_neg, df_pos)
    print("Determistics Data Match completed!!! " + str(datetime.now()))
    print(len(df_cust), len(df_neg), len(df_pos))
    print("Data load of DDM file started: " + str(datetime.now()))
    intFileDir = cwd + r"\\IntermediateFiles\\DDM\\"
    DDMFile = r"DDM.csv"
    peCustFile = "DDM_" + custFile
    MatchedFiles(intFileDir, DDMFile, index_df)
    MatchedFiles(intFileDir, peCustFile, df_cust)
    MatchedFiles(intFileDir, custFile, df_cust)
    IntermediateFiles(intFileDir, negFile, df_neg)
    IntermediateFiles(intFileDir, posFile, df_pos)
    print("Data load of DDM file completed!!! " + str(datetime.now()))
