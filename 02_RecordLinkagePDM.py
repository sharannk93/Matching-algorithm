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

This script performs probablistic data matching between customer list and negative/positive list. It performs below mentioned steps:
01. Reads data from the files generated after PDM completion
02. Matches data using different determistics record linkage rules
03. Generates files under PDM folder present under IntermediateFiles folder

"""

# Load required packages
import os
import pandas as pd
import recordlinkage
from datetime import datetime


def extractSource(dir, files):
    ''' 
    Function to extract Data from files in a specific format
    '''
    t = {"FIRST_NAME": object, "FIRST_NAME": object, "LAST_NAME": object, "DOB": object, "STREET": object, "ZIP": object, "CITY": object, "HNRNEW": object}
    filename = dir + files
    df = pd.read_csv(filename, index_col = "ID", na_values = ['1900-00-00', '1800-00-00', '1700-00-00', '-99999', '-88888', '-77777'], dtype=t)
    df['DOB'] = pd.to_datetime(df['DOB'], format='%Y-%m-%d')
    return df


def combineName(df):
    '''
    Function to combine first name with last name and store the values into name
    '''
    df['NAME'] = df['FIRST_NAME'] + ' ' + df['LAST_NAME']
    #df = df.drop(columns = ['FIRST_NAME', 'LAST_NAME'], axis = 1)
    return df


def combineAddress(df):
    '''
    Function to combine House Number with street name and store the values into address
    '''
    df['ADDRESS'] = df['HNRNEW'] + ' ' + df['STREET']
    #df = df.drop(columns = ['HNRNEW', 'STREET'], axis = 1)
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


def indexBlocker(index, df, lst):
    '''
    Function to create index before performing PDM for quick completion
    '''
    indexer = recordlinkage.Index()
    indexer.block(left_on = index, right_on = index)
    candidates = indexer.index(df, lst)
    return candidates


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


def recordMatchesPDM(candidates, df, lst, exactCols, partialCols):
    '''
    Function to get potential matches from customer list and positive/negative list using Jarowinkler algorithm with 76% and above similarity
    '''
    compare = recordlinkage.Compare()
    for col in exactCols:
        lbl = col + '_SCORE'
        compare.exact(col, col, label=lbl)
    for col in partialCols:
        lbl = col + '_SCORE'
        compare.string(col, col, method='jarowinkler', threshold = 0.76, label = lbl)
    features = compare.compute(candidates, df, lst)
    pot_matches = features[features.sum(axis=1) > 1].reset_index()
    pot_matches['SCORE'] = pot_matches.iloc[:, 2:].sum(axis = 1)
    numberOfMatches = len(exactCols) + len(partialCols)
    pot_matches = pot_matches[pot_matches['SCORE'] == numberOfMatches]
    return pot_matches


def colMatchPDMPOS(cust, pos, index, exact, partial, i, score):
    '''
    Function to match customers with the positive list based on a condition
    '''
    pos_candidates = indexBlocker(index, cust, pos)
    pos_matches = recordMatchesPDM(pos_candidates, cust, pos, exact, partial)
    pos_matches = pos_matches.rename(columns = {'ID_1':'ID_CUST', 'ID_2': 'ID_POS'})
    pos_matches = pos_matches[['ID_CUST', 'ID_POS']]
    matchCriteria = "PDM RULE" + str(i) + ": Exact Matches on [" + ', '.join(exact) + '] AND Partial Matches on [' + ', '.join(partial) + ']'
    pos_matches["MATCH_CRITERIA"] = matchCriteria
    pos_matches["MATCH_SCORE"] = float(score[0])
    matched_index = pos_matches[["ID_CUST", "ID_POS", "MATCH_CRITERIA", "MATCH_SCORE"]]
    matched_index.sort_values(by=["ID_CUST"], inplace=True)
    matched_index = matched_index.reset_index()
    matched_index = matched_index.drop("index", axis = 1)
    cust = cust.reset_index()
    #cust = cust.drop("index", axis = 1)
    cust = cust[~cust["ID"].isin(matched_index["ID_CUST"])]
    cust.set_index("ID", inplace = True)
    return matched_index, cust

def colMatchPDMNEG(cust, neg, index, exact, partial, i, score):
    '''
    Function to match customer with the negative list based on a condition
    '''
    neg_candidates = indexBlocker(index, cust, neg)
    neg_matches = recordMatchesPDM(neg_candidates, cust, neg, exact, partial)
    neg_matches = neg_matches.rename(columns = {'ID_1':'ID_CUST', 'ID_2': 'ID_NEG'})
    neg_matches = neg_matches[['ID_CUST', 'ID_NEG']]
    matchCriteria = "PDM RULE" + str(i) + ": Exact Matches on [" + ', '.join(exact) + '] AND Partial Matches on [' + ', '.join(partial) + ']'
    neg_matches["MATCH_CRITERIA"] = matchCriteria
    neg_matches["MATCH_SCORE"] = float(score[0])
    matched_index = neg_matches[["ID_CUST", "ID_NEG", "MATCH_CRITERIA", "MATCH_SCORE"]]
    matched_index.sort_values(by=["ID_CUST"], inplace=True)
    matched_index = matched_index.reset_index()
    matched_index = matched_index.drop("index", axis = 1)
    cust = cust.reset_index()
    #cust = cust.drop("index", axis = 1)
    cust = cust[~cust["ID"].isin(matched_index["ID_CUST"])]
    cust.set_index("ID", inplace = True)
    return matched_index, cust


def PDM(cust_df, neg_df, pos_df):
    '''
    Function to iterativly perform PDM for all the defined rules
    '''
    cust_df = combineName(cust_df)
    cust_df = combineAddress(cust_df)
    neg_df = combineName(neg_df)
    neg_df = combineAddress(neg_df)
    pos_df = combineName(pos_df)
    pos_df = combineAddress(pos_df)
    cust_pos_df = cust_df.copy()
    cust_neg_df = cust_df.copy()
    print("Start PDM")
    condition1 = [['FIRST_NAME', 'LAST_NAME', 'CITY'], ['FIRST_NAME', 'LAST_NAME', 'CITY', 'STREET'], ['ZIP'], [81.5]]
    condition2 = [['LAST_NAME', 'CITY', 'ZIP'], ['LAST_NAME', 'CITY', 'ZIP', 'STREET'], ['FIRST_NAME'], [81]]
    condition3 = [['FIRST_NAME', 'CITY', 'ZIP'], ['FIRST_NAME', 'CITY', 'ZIP', 'STREET'], ['LAST_NAME'], [80.5]]
    condition4 = [['FIRST_NAME', 'LAST_NAME', 'CITY', 'ZIP'], ['FIRST_NAME', 'LAST_NAME', 'CITY', 'ZIP'], ['STREET'], [79]]
    condition5 = [['STREET', 'CITY', 'ZIP','HNRNEW'], ['STREET', 'CITY', 'ZIP','HNRNEW'], ['FIRST_NAME', 'LAST_NAME'], [78.5]]
    condition6 = [['DOB', 'LAST_NAME'], ['DOB', 'LAST_NAME'], ['FIRST_NAME'], [78]]
    condition7 = [['FIRST_NAME', 'LAST_NAME', 'ZIP'], ['FIRST_NAME', 'LAST_NAME', 'ZIP', 'STREET'], ['CITY'], [82]]
    condition8 = [['DOB', 'FIRST_NAME'], ['DOB', 'FIRST_NAME'], ['LAST_NAME'], [77.5]]
    condition9 = [['FIRST_NAME', 'LAST_NAME'], ['FIRST_NAME', 'LAST_NAME'], ['STREET', 'CITY', 'ZIP'], [75]]
    condition10 = [['CITY', 'ZIP'], ['CITY', 'ZIP'], ['FIRST_NAME', 'LAST_NAME', 'STREET', 'HNRNEW'], [74]]
    condition11 = [['ZIP'], ['ZIP'], ['FIRST_NAME', 'LAST_NAME', 'CITY', 'STREET', 'HNRNEW'], [73]]
    matchConditions = [condition1, condition2, condition3, condition4, condition5, condition6, condition7, condition8, condition9, condition10, condition11]
    cwd = os.getcwd()
    intFileDir = cwd + r"\\IntermediateFiles\\PDM\\"
    i = 1
    custFile = r"00_List_Customer_Monitoring.csv"
    matched_idx = pd.DataFrame()
    for index, exactCols, partialCols, matchScore in matchConditions:
        print("Start of Rule" + str(i) + ':-')
        print("Exact Match: [" + ', '.join(exactCols) + "]")
        print('Partial Match: [' + ', '.join(partialCols) + "]")
        FileNamePOS = "PDM_POS_Rule" + str(i) + ".csv"
        FileNameNEG = "PDM_NEG_Rule" + str(i) + ".csv"
        custFilePOSPostMatch = "PDM_POS_Rule" + str(i) + "_" + custFile
        custFileNEGPostMatch = "PDM_NEG_Rule" + str(i) + "_" + custFile
        matchCriteria = "RULE" + str(i) + ": Exact Matches on [" + ', '.join(exactCols) + '] AND Partial Matches on [' + ', '.join(partialCols) + ']'
        idxPOS, cust_pos_df = colMatchPDMPOS(cust_pos_df, pos_df, index, exactCols, partialCols, i, matchScore)
        idxNEG, cust_neg_df = colMatchPDMNEG(cust_neg_df, neg_df, index, exactCols, partialCols, i, matchScore)
        MatchedFiles(intFileDir, FileNamePOS, idxPOS)
        MatchedFiles(intFileDir, custFilePOSPostMatch, cust_pos_df)
        MatchedFiles(intFileDir, FileNameNEG, idxNEG)
        MatchedFiles(intFileDir, custFileNEGPostMatch, cust_neg_df)
        matched_idx = matched_idx.append(matchedIndex(idxNEG, idxPOS, matchCriteria), ignore_index=True, sort=False)
        matched_idx.sort_values(by=["ID_CUST"], inplace=True)
        print("End of Rule" + str(i) + '!!!')
        i += 1
    matched_idx.sort_values(by=["ID_CUST"], inplace=True)
    matched_idx = matched_idx.reset_index()
    matched_idx = matched_idx.drop("index", axis = 1)
    cust_df = cust_df.reset_index()
    #cust_df = cust_df.drop("index", axis = 1)
    cust_df = cust_df[~cust_df["ID"].isin(matched_idx["ID_CUST"])]
    cust_df.set_index("ID", inplace = True)
    print("End of PDM!!!")
    return matched_idx, cust_df


# Main function - starting point of the script
if __name__ == "__main__":
    print("Data Load started: " + str(datetime.now()))
    cwd = os.getcwd()
    intFileDir = cwd + r"\\IntermediateFiles\\DDM\\"
    print("CUSTOMER MONITORING LIST")
    custFile = r"00_List_Customer_Monitoring.csv"
    df_cust = extractSource(intFileDir, custFile)
    print("NEGATIVE LIST")
    negFile = r"01a_List_Negative.csv"
    df_neg = extractSource(intFileDir, negFile)
    print("POSITIVE LIST")
    posFile = r"01b_List_Positive.csv"
    df_pos = extractSource(intFileDir, posFile)
    print("Data Load Completed!!! " + str(datetime.now()))
    print(len(df_cust), len(df_neg), len(df_pos))
    print("Probablistic Data Match started: " + str(datetime.now()))
    index_df, df_cust = PDM(df_cust, df_neg, df_pos)
    print("Probablistic Data Match completed!!! " + str(datetime.now()))
    print(len(df_cust), len(df_neg), len(df_pos))
    print("Data load of PDM file started: " + str(datetime.now()))
    intFileDir = cwd + r"\\IntermediateFiles\\PDM\\"
    DDMFile = r"PDM.csv"
    peCustFile = "PDM_" + custFile
    MatchedFiles(intFileDir, DDMFile, index_df)
    IntermediateFiles(intFileDir, peCustFile, df_cust)
    IntermediateFiles(intFileDir, custFile, df_cust)
    IntermediateFiles(intFileDir, negFile, df_neg)
    IntermediateFiles(intFileDir, posFile, df_pos)
    print("Data load of PDM file completed!!! " + str(datetime.now()))
