import networkx as nx
import requests
#import requests_html
import scipy
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from pandas.io.json import json_normalize
import csv
import re
import json
import urllib
import urllib.request as request 
from urllib.request import Request, urlopen
import os 
from collections import ChainMap
from time import sleep
#****************************************************************
#Title: Bitcoin blockchain data extract
#Author: Adam Turner
#Date: 31.03.2021
#Version: 4.2
#Availability: <>
#****************************************************************
#****************************************************************
#API reference: walletexplorer.com
#http://www.walletexplorer.com/api/1/address-lookup?address=16SbwNa22nBwhLtg6HzWVYFQiUxtNzAUpt&caller=AT
#http://www.walletexplorer.com/api/1/wallet-addresses?wallet=bitstamp&from=0&count=100&caller=AT
#http://www.walletexplorer.com/api/1/tx?txid=99fd988bf60ff67847488ceeb76d08a8fcca7bde80bb0b06be2ef4a0055c3ba7&caller=AT
#http://www.walletexplorer.com/api/1/address?address=1BitcoinEaterAddressDontSendf59kuE&from=0&count=100&caller=AT
#http://www.walletexplorer.com/api/1/wallet?wallet=bitstamp&from=0&count=100&caller=AT
#http://www.walletexplorer.com/api/1/firstbits?prefix=1bitcoin&caller=AT
#****************************************************************
#Step 0. Using the Wallet Explorer API - Download the list of addresses JSON file
#run the download_module() to produce a transaction history of the seed addresses we wish to trace
#Code depends on the transaction history file having already been downloaded from the walletexplorer.com website in CSV format and available in the USER_DIR_PATH
#This function only needs to be run once off in order to create the transaction listing file for the rest of the program to use
#http://www.walletexplorer.com/api/1/address?address=12t9YDPgwueZ9NyMgw519p7AA8isjr6SMw&from=0&count=100&caller=AT
#****************************************************************
def download_module():
    #Set BTC ransom seed addresses
    ##WannaCry
    #base_seeds=['12t9YDPgwueZ9NyMgw519p7AA8isjr6SMw', '13AM4VW2dhxYgXeQepoHkHSQuy6NgaEb94', '115p7UMMngoj1pMvkpHijcRdfJNXj6LrLn']
    ##CryptoDefense
    #base_seeds=['19DyWHtgLgDKgEeoKjfpCJJ9WU8SQ3gr27', '1EmLLj8peW292zR2VvumYPPa9wLcK4CPK1']
    ##NotPetya
    #base_seeds=['1Mz7153HMuxXTuR2R1t78mGSdzaAtNbBWX']
    ##Control Seed
    base_seeds=['1HesYJSP1QqcyPEjnQ9vzBL1wujruNGe7R']
    #File Path
    path='USER_DIR_PATH'
        #loop through the list of base_seeds addresses declared above 
    for aSeed in base_seeds:
        file_name = aSeed+"_tx_history"
        df = pd.read_csv(path+file_name+".csv")
        export_to_JSON(path, file_name, df)

#****************************************************************
#Step 1. Extraction of the network created by the  base_seed addresses
#run the data_prep() module to produce the cash_in and cash_out network files
#Code depends on the exisistance of the transaction history JSON file created from download_module()
#This function needs to be run for every cash_in and cash_out network you wish to create from a set of base_seed addresses
#****************************************************************
def data_prep():
    #File Path
    path='USER_DIR_PATH'
    #Set base_seed addresses for the program to use
    ##WannaCry
    base_seeds=['12t9YDPgwueZ9NyMgw519p7AA8isjr6SMw']
    #base_seeds=['12t9YDPgwueZ9NyMgw519p7AA8isjr6SMw', '13AM4VW2dhxYgXeQepoHkHSQuy6NgaEb94', '115p7UMMngoj1pMvkpHijcRdfJNXj6LrLn']
    ##CryptoDefense
    #base_seeds=['1EmLLj8peW292zR2VvumYPPa9wLcK4CPK1']
    #base_seeds=['19DyWHtgLgDKgEeoKjfpCJJ9WU8SQ3gr27', '1EmLLj8peW292zR2VvumYPPa9wLcK4CPK1']
    ##NotPetya
    #base_seeds=['1Mz7153HMuxXTuR2R1t78mGSdzaAtNbBWX']
    ##Control
    #base_seeds=['1HesYJSP1QqcyPEjnQ9vzBL1wujruNGe7R']
    #Initialise transaction counter and network depth parameters
    tx_count = 0
    START_DEPTH = 0
    MAX_DEPTH = 3
    #for each seed address, iterate through its transactions
    for aSeed in base_seeds:
        #open the JSON files containing the transaction listing corresponding to the base_seed addresses
        with open('USER_DIR_PATH'+aSeed+'_tx_history.json', 'r') as f:
            #load the transaction listing file into a dict object
            data = json.load(f)
            #Create empty dict to read in the walletexplorer API response
            d = {}
            #Create empty list to append a list of dicts for every response from the API 
            content_in = []
            content_out = []
            content_2 = []
            #Iterate through the transactions relating to the base_seed addresses (this is the transaction catalogue)
            for tx in data:
                    #Test if the transaction is PAYING INTO the seed address OR... 
                    #...PAYING OUT from the seed address
                    #...IGNORE change address condition, this is handled at a lower level
                    if tx['description'] == "PAYMENT RECEIVED":
                        tx_id = tx['tx hash'] #UNCOMMENT WHEN FINISHED LOOKING AT CASH OUT XXXXXX
                        #print("Inflows " +tx_id)
                        content_in=transaction_cat(tx_id, d, content_in)
                        #Increment flow control count
                        tx_count = tx_count + 1
                        #break
                    elif tx['description'] == "PAYMENT SENT":
                        tx_id = tx['tx hash']
                        #print("Found a cash out " +tx_id)
                        content_out=transaction_cat(tx_id, d, content_out)
                        #Increment flow control count
                        tx_count = tx_count + 1
                        #break
                        
            #Create a DataFrame with appended content_in
            df_rx = pd.DataFrame(content_in)
            df_rx.rename(columns={'in':'ins'}, inplace=True)
            
            #Create DataFrame with appended content_out   
            df_sent = pd.DataFrame(content_out)
            df_sent.rename(columns={'in':'ins'}, inplace=True)
            
            print("****** Seed Level Dataframe - CASH-IN ******")
            #print(df_rx)
            ##Call the unwind_df_ins module to unwind the nested dataframe in order to produce the linked network files in JSON format 
            print("****** Calling: UNWIND_DF_INS ******")
            unwind_df_ins(df_rx, MAX_DEPTH, START_DEPTH, aSeed, path, content_in, content_2)
            
            print("****** Seed Level Dataframe - CASH-OUT ******")
            ##Call the unwind_df_outs module to unwind the nested dataframe in order to produce the linked network files in JSON format
            #Test to see if the seed address has started to cash out
            if(df_sent.empty):
                print("****** NO CASH OUT ACTIVITY IN TRANSACTION LISTING ******")
                return
            else:
                print("****** Calling: UNWIND_DF_OUTS ******")
                unwind_df_outs(df_sent, MAX_DEPTH, START_DEPTH, aSeed, path, content_out, content_2)
            
            return

#****************************************************************
#Support Function: transaction_cat() takes a transaction id and appends the response from the API to the list of dictionaries collected 
#also calls the download_data_prep() module to manage the API request
#****************************************************************
def transaction_cat(tx_id, d, content):
    API_URL_TXS = "http://www.walletexplorer.com/api/1/tx?txid="+tx_id+"&caller=AT"
    #print("URL:" + API_URL_TXS)
    #call api and return dictionary response
    d = download_data_prep(API_URL_TXS)    
    #append dictionary to list of dicts
    content.append(d)
    return content

#****************************************************************
#Step 2. unwind_df_ins() 
#manage the dataframe for the ins and outs transactions at each network depth
#----------------------Parameter Definitions----------------------
##df - Unwound lower level DF 
##MAX_DEPTH - Limit to the number of levels deep to trace the cash-out network (depth away from the seed address)
##CURR_DEPTH_INS - The current depth level on the cash-out network (depth away from the seed address)
#aSeed - seed address being analysed
#path - for file management
#content_in - List type of the lower level DF
#content_2 - List type of the top level DF    
#****************************************************************
def unwind_df_ins(df, MAX_DEPTH, CURR_DEPTH_INS, aSeed, path, content_in, content_2):
    
    print("****** Running: UNWIND_DF_INS ******")
    print("****** MAX DEPTH = " +str(MAX_DEPTH))
    print("****** CURRENT DEPTH = " +str(CURR_DEPTH_INS))
    #take the DataFrame (df) containing the inputs of the transaction, unwind (normalize) and write them to the df_ins DataFrame
    df_ins = pd.concat([pd.DataFrame(json_normalize(x)) for x in df['ins']] ,ignore_index=True)
    #print(df_ins)
    
    #Test if this is the first transaction from the seed address and only add and initialise once
    #Add a new column to the dataframe to record network depth and initialise it to 0
    if int(CURR_DEPTH_INS) < 1:
        df['depth_'] = '0'
        df_ins['depth_'] = '0'
        #Add new column to dataframe to record whether the transaction has been read isRead=False
        df_ins['isRead'] = 'False'
        print("##########################################")
        print("****** IF CURR_DEPTH_OUTS < 1 ******")
        print("##########################################")

        print("****** TOP LEVEL DF - with lists of ins/outs ******")
        #print(content)
        #print(df)
        df_content = []
        df_dict = df.to_dict('index')
        df_content.append(df_dict)
        print("****** TOP LEVEL DF - APPENDING CONTENT ******")
        #print(df_content)
        
        print("****** Level "+str(CURR_DEPTH_INS)+" - Top Level DF ******")
        print(df)
        print("##################################################################################################")
        
        print("****** LOWER LEVEL DF - with unwound lists of ins/outs ******")
        df_ins_content = []
        df_ins_dict = df_ins.to_dict('index')
        df_ins_content.append(df_ins_dict)
        print("****** Lower Level DataFrame - APPENDING CONTENT")
        #print(df_ins_content)

        print("****** Unwinding LOWER LEVEL DF - with appended content *******")
        df_ins_1 = pd.DataFrame(df_ins_content).T
        df_ins_1.columns = ['ins']
        print(df_ins_1)
        df_ins = pd.concat([pd.DataFrame(json_normalize(x)) for x in df_ins_1['ins']] ,ignore_index=True)

        print("***** Level "+str(CURR_DEPTH_INS)+" - LOWER LEVEL SEED DF ******")
        print(df_ins)
        #return
        #----------------------
        #Parameter Definitions
        #----------------------
        #df_ins - Unwound lower level DF 
        #MAX_DEPTH - Limit to the number of levels deep to trace the cash-out network (depth away from the seed address)
        #CURR_DEPTH_OUTS - The current depth level on the cash-out network (depth away from the seed address)
        #aSeed - seed address being analysed
        #path - for file management
        #df_ins_content - List type of the lower level DF
        #df_content - List type of the top level DF
        ##Call df_ins_depth() module to link transactions together all the way to MAX_DEPTH hops from the seed address
        df_ins_depth(df_ins, MAX_DEPTH, CURR_DEPTH_INS, aSeed, path, df_ins_content, df_content)
        #return
        #CURR_DEPTH_INS = CURR_DEPTH_INS + 1
        
    else:

        df['depth_'] = str(CURR_DEPTH_INS)
        df_ins['depth_'] = str(CURR_DEPTH_INS)
        #Add new column to dataframe to record whether the transaction has been read isRead=False
        df_ins['isRead'] = 'False'
        print("****** IF CURR_DEPTH_INS >= 1 ******")        
        print("****** TOP LEVEL DF - with lists of ins/outs ******")
        print("****** CONCAT TOP LEVEL DF - with appended content ******")
        #Convert content_2 list to data frame
        df_1 = pd.DataFrame(content_2).T
        print("CONTENT_2 - NOT APPENDED")
        #Give the column a name 'ins' for input transactions
        df_1.columns = ['ins']
        #print(df_1)
        #Unwind content_2 list into columns and concatenate with latest dataframe from df_ins_depth()
        df_2 = pd.concat([pd.DataFrame(json_normalize(x)) for x in df_1['ins']] ,ignore_index=True)
        df_3 = pd.concat([df, df_2], ignore_index=True, sort=True)
        print("****** Level "+str(CURR_DEPTH_INS)+" - TOP LEVEL DF - TO BE USED IN NEO4J ******")
        #This is the dataframe we want to load into Neo4j, this DataFrame must exported to JSON
        print(df_3)
        #EXPORT_TO_JSON()
        file_name = aSeed+"_txs_ins"
        print("****** Exporting Top Level dataframe to JSON ******" +file_name)
        export_to_JSON(path, file_name, df_3)

        #Re-initalise the df_ins_content list
        df_content = []
        df_dict = df_3.to_dict('index')
        #read in the new dataframe
        df_content.append(df_dict)
        print("****** TOP LEVEL DF - Convert back to a list of dicts to send through to df_ins_depth() ******")
        #print(df_content)
        print("##################################################################################################")
        
        print("****** Unwinding LOWER LEVEL DataFrame - with appended content ******")
        df_ins_1 = pd.DataFrame(content_in).T
        df_ins_1.columns = ['ins']
        print("****** LOWER LEVEL DF - APPENDING CONTENT")
        #print(df_ins_1)
        df_ins_2 = pd.concat([pd.DataFrame(json_normalize(x)) for x in df_ins_1['ins']] ,ignore_index=True)
        df_ins_3 = pd.concat([df_ins, df_ins_2], ignore_index=True, sort=True)
        print("****** Level "+str(CURR_DEPTH_INS)+" - LOWER LEVEL DF ******")
        print(df_ins_3)
        
        #EXPORT_TO_JSON()
        file_name = aSeed+"_txs_ins_lower"#+str(START_DEPTH)
        print("****** Exporting Top Level dataframe to JSON ******" +file_name)
        export_to_JSON(path, file_name, df_ins_3)

        #Re-initalise the df_ins_content list
        df_ins_content = []
        df_ins_dict = df_ins_3.to_dict('index')
        #read in the new dataframe
        df_ins_content.append(df_ins_dict)
        print("****** LOWER LEVEL - Convert back to a list of dicts to send through to df_ins_depth() ******")
        #print(df_ins_content)
        #df_ins['isRead'] = 'False'
        #----------------------
        #Parameter Definitions
        #----------------------
        #df_INs_3 - Unwound lower level DF 
        #MAX_DEPTH - Limit to the number of levels deep to trace the cash-out network (depth away from the seed address)
        #CURR_DEPTH_INS - The current depth level on the cash-out network (depth away from the seed address)
        #aSeed - seed address being analysed
        #path - for file management
        #df_ins_content - List type of the lower level DF
        #df_content - List type of the top level DF
        ##Call df_ins_depth() module to link transactions together all the way to MAX_DEPTH hops from the seed address
        df_ins_depth(df_ins, MAX_DEPTH, CURR_DEPTH_INS, aSeed, path, df_ins_content, df_content)

#****************************************************************
#Step 2. unwind_df_outs() 
#manage the dataframe for the ins and outs transactions at each network depth
#----------------------Parameter Definitions----------------------
##df - Unwound lower level DF 
##MAX_DEPTH - Limit to the number of levels deep to trace the cash-out network (depth away from the seed address)
##CURR_DEPTH_OUTS - The current depth level on the cash-out network (depth away from the seed address)
#aSeed - seed address being analysed
#path - for file management
#content_out - List type of the lower level DF
#content_2 - List type of the top level DF    
#****************************************************************
def unwind_df_outs(df, MAX_DEPTH, CURR_DEPTH_OUTS, aSeed, path, content_out, content_2):
  
    print("****** Running: UNWIND_DF_OUTS ******")
    print("****** MAX DEPTH = " +str(MAX_DEPTH))
    print("****** CURRENT DEPTH = " +str(CURR_DEPTH_OUTS))
    #take the DataFrame (df) containing the outputs of the transaction, unwind (normalize) and write them to the df_outs DataFrame
    df_outs = pd.concat([pd.DataFrame(json_normalize(x)) for x in df['out']] ,ignore_index=True)
    
    #Test if this is the first transaction from the seed address and only add and initialise once
    #Add a new column to the dataframe to record network depth and initialise it to 0
    if int(CURR_DEPTH_OUTS) < 1:
        df['depth_'] = '0'
        df_outs['depth_'] = '0'
        #Add new column to dataframe to record whether the transaction has been read isRead=False
        df_outs['isRead'] = 'False'
        print("##########################################")
        print("****** IF CURR_DEPTH_OUTS < 1 ******")
        print("##########################################")

        print("****** TOP LEVEL DF - with lists of ins/outs ******")
        #print(content)
        #print(df)
        df_content = []
        df_dict = df.to_dict('index')
        df_content.append(df_dict)
        print("****** TOP LEVEL DF - APPENDING CONTENT ******")
        #print(df_content)
        
        print("****** Level "+str(CURR_DEPTH_OUTS)+" - TOP LEVEL SEED DF ******")
        print(df)
        print("##################################################################################################")
        
        print("****** LOWER LEVEL DF - with unwound lists of ins/outs ******")
        df_outs_content = []
        df_outs_dict = df_outs.to_dict('index')
        df_outs_content.append(df_outs_dict)
        print("****** LOWER LEVEL DF - APPENDING CONTENT")
        #print(df_outs_content)

        print("****** Unwinding LOWER LEVEL DF - with appended content ******")
        df_outs_1 = pd.DataFrame(df_outs_content).T
        print(df_outs_1)
        df_outs_1.columns = ['outs']
        #print(df_outs_1)
        df_outs = pd.concat([pd.DataFrame(json_normalize(x)) for x in df_outs_1['outs']] ,ignore_index=True)

        print("****** Level "+str(CURR_DEPTH_OUTS)+" - LOWER LEVEL SEED DF ******")
        print(df_outs)
        #return
        #----------------------
        #Parameter Definitions
        #----------------------
        #df_outs - Unwound lower level DF 
        #MAX_DEPTH - Limit to the number of levels deep to trace the cash-out network (depth away from the seed address)
        #CURR_DEPTH_OUTS - The current depth level on the cash-out network (depth away from the seed address)
        #aSeed - seed address being analysed
        #path - for file management
        #df_outs_content - List type of the lower level DF
        #df_content - List type of the top level DF
        ##Call df_outs_depth() module to link transactions together all the way to MAX_DEPTH hops from the seed address
        df_outs_depth(df_outs, MAX_DEPTH, CURR_DEPTH_OUTS, aSeed, path, df_outs_content, df_content)
        
    else:
        
        df['depth_'] = str(CURR_DEPTH_OUTS)
        df_outs['depth_'] = str(CURR_DEPTH_OUTS)
        #Add new column to dataframe to record whether the transaction has been read isRead=False
        df_outs['isRead'] = 'False'
        print("****** IF CURR_DEPTH_OUTS >= 1 ******")        
        print("****** TOP LEVEL DF - with lists of ins/outs ******")
        print("****** CONCAT TOP LEVEL DF - with appended content ******")
        #Convert content_2 list to data frame
        df_1 = pd.DataFrame(content_2).T
        print("CONTENT_2 - NOT APPENDED")
        #Give the column a name 'OUTS' for input transactions
        df_1.columns = ['outs']
        #print(df_1)
        #Unwind content_2 list into columns and concatenate with latest dataframe from df_outs_depth()
        df_2 = pd.concat([pd.DataFrame(json_normalize(x)) for x in df_1['outs']] ,ignore_index=True)
        df_3 = pd.concat([df, df_2], ignore_index=True, sort=True)
        print("****** Level "+str(CURR_DEPTH_OUTS)+" - TOP LEVEL DF - TO BE USED IN NEO4J ******")
        #This is the dataframe we want to load into Neo4j, this DataFrame must exported to JSON
        print(df_3)
        #EXPORT_TO_JSON()
        file_name = aSeed+"_txs_outs"#+str(START_DEPTH)
        print("****** Exporting TOP LEVEL DF to JSON ******" +file_name)
        export_to_JSON(path, file_name, df_3)

        #Re-initalise the df_outs_content list
        df_content = []
        df_dict = df_3.to_dict('index')
        #read in the new dataframe
        df_content.append(df_dict)
        print("****** TOP LEVEL DF - Convert back to a list of dicts to send through to df_outs_depth() ******")
        #print(df_content)
        print("##################################################################################################")

        print("****** Unwinding LOWER LEVEL DataFrame - with appended content *******")
        df_outs_1 = pd.DataFrame(content_out).T
        df_outs_1.columns = ['outs']
        print("****** LOWER LEVEL DF - APPENDING CONTENT")
        #print(df_ins_1)
        df_outs_2 = pd.concat([pd.DataFrame(json_normalize(x)) for x in df_outs_1['outs']] ,ignore_index=True)
        df_outs_3 = pd.concat([df_outs, df_outs_2], ignore_index=True, sort=True)
        print("****** Level "+str(CURR_DEPTH_OUTS)+" - LOWER LEVEL DF ******")
        print(df_outs_3)
        
        #EXPORT_TO_JSON()
        file_name = aSeed+"_txs_outs_lower"#+str(START_DEPTH)
        print("****** Exporting LOWER LEVEL DF to JSON ******" +file_name)
        export_to_JSON(path, file_name, df_outs_3)

        #Re-initalise the df_out_content list
        df_outs_content = []
        df_outs_dict = df_outs_3.to_dict('index')
        #read in the new dataframe
        df_outs_content.append(df_outs_dict)
        print("****** LOWER LEVEL - Convert back to a list of dicts to send through to df_outs_depth() ******")
        #df_ins['isRead'] = 'False'
        #----------------------
        #Parameter Definitions
        #----------------------
        #df_outs_3 - Unwound lower level DF 
        #MAX_DEPTH - Limit to the number of levels deep to trace the cash-out network (depth away from the seed address)
        #CURR_DEPTH_OUTS - The current depth level on the cash-out network (depth away from the seed address)
        #aSeed - seed address being analysed
        #path - for file management
        #df_outs_content - List type of the lower level DF
        #df_content - List type of the top level DF
        ##Call df_outs_depth() module to link transactions together all the way to MAX_DEPTH hops from the seed address
        df_outs_depth(df_outs, MAX_DEPTH, CURR_DEPTH_OUTS, aSeed, path, df_outs_content, df_content)

#****************************************************************
#Step 3. df_ins_depth() 
#manage the dataframe for the ins and outs transactions at each network depth
#----------------------Parameter Definitions----------------------
##df - Unwound lower level DF 
##MAX_DEPTH - Limit to the number of levels deep to trace the cash-out network (depth away from the seed address)
##CURR_DEPTH_OUTS - The current depth level on the cash-out network (depth away from the seed address)
#aSeed - seed address being analysed
#path - for file management
#content_out - List type of the lower level DF
#content_2 - List type of the top level DF    
#****************************************************************      
def df_ins_depth(df_ins, MAX_DEPTH, CURR_DEPTH_INS, aSeed, path, df_ins_content, df_content):
    print("****** Running: DF_INS_DEPTH ******")
    print("****** MAX DEPTH = " +str(MAX_DEPTH))
    print("****** CURR DEPTH = " +str(CURR_DEPTH_INS))
    d={}
    content=[]
    print("****** DF_INS ******")
    print(df_ins['next_tx'])    
    #Loop through MAX_DEPTH on each 'in' transaction 
    for idx, tx in enumerate(df_ins['next_tx']):
    #do not call API_TX with empty tx
        if str(tx) == "nan":
            CURR_DEPTH_INS = CURR_DEPTH_INS + 1
        else:
            if int(CURR_DEPTH_INS) > int(MAX_DEPTH):
                print("XXXXXX MAX_DEPTH EXCEEDED XXXXXX")
                print("****** CURR DEPTH = " +str(CURR_DEPTH_INS)+" is > MAX DEPTH = " +str(MAX_DEPTH)+" ******")
                break

            #call API_TX with tx
            print("****** INSIDE DF_INS_DEPTH FOR LOOP ******")
            print("****** Call download_data_prep(API_URL_TXS) ******")
            #print("****** TX: " +str(tx)+ " at depth ["+str(CURR_DEPTH_INS)+"] ******")
            print("****** TX: " +str(tx)+ " at position ["+str(idx)+"] ******")
            content=transaction_cat(tx, d, content)
            #Return dictionary append dictionary to list of dicts
            #Create DataFrame with appended content   !!!!!!NEED TO APPEND ALL TXS FROM DF_OUTS 
            df = pd.DataFrame(content)
            df.rename(columns={'in':'ins'}, inplace=True)
    
    #Sending the dataframe containing the collected transactions to be unwound and added to the JSON file
    CURR_DEPTH_INS = CURR_DEPTH_INS + 1
    if int(CURR_DEPTH_INS) > int(MAX_DEPTH):
        print("XXXXXX MAX_DEPTH EXCEEDED XXXXXX")
        print("****** CURR DEPTH = " +str(CURR_DEPTH_INS)+" is > MAX DEPTH = " +str(MAX_DEPTH)+" ******")
    else: 
        unwind_df_ins(df, MAX_DEPTH, CURR_DEPTH_INS, aSeed, path, df_ins_content, df_content)         

#****************************************************************
#Step 3. df_outs_depth() 
#manage the dataframe for the ins and outs transactions at each network depth
#----------------------Parameter Definitions----------------------
##df - Unwound lower level DF 
##MAX_DEPTH - Limit to the number of levels deep to trace the cash-out network (depth away from the seed address)
##CURR_DEPTH_OUTS - The current depth level on the cash-out network (depth away from the seed address)
#aSeed - seed address being analysed
#path - for file management
#content_out - List type of the lower level DF
#content_2 - List type of the top level DF    
#**************************************************************** 
def df_outs_depth(df_outs, MAX_DEPTH, CURR_DEPTH_OUTS, aSeed, path, df_outs_content, df_content):
    print("****** Running: DF_OUTS_DEPTH ******")
    print("****** MAX DEPTH = " +str(MAX_DEPTH))
    print("****** CURR DEPTH = " +str(CURR_DEPTH_OUTS))
    d={}
    content=[]
    print("****** DF_OUTS ******")
    print(df_outs['next_tx'])    
    #Loop through MAX_DEPTH on each 'out' transaction 
    for idx, tx in enumerate(df_outs['next_tx']):
        #do not call API_TX with empty tx
            if str(tx) == "nan":
                CURR_DEPTH_OUTS = CURR_DEPTH_OUTS + 1
            else:
                if int(CURR_DEPTH_OUTS) > int(MAX_DEPTH):
                    print("XXXXXX MAX_DEPTH EXCEEDED XXXXXX")
                    print("****** CURR DEPTH = " +str(CURR_DEPTH_OUTS)+" is > MAX DEPTH = " +str(MAX_DEPTH)+" ******")
                    break

                #call API_TX with tx
                print("****** INSIDE DF_OUTS_DEPTH FOR LOOP ******")
                print("****** Call download_data_prep(API_URL_TXS) ******")
                #print("****** TX: " +str(tx)+ " at depth ["+str(CURR_DEPTH_OUTS)+"] ******")
                print("****** TX: " +str(tx)+ " at position ["+str(idx)+"] ******")
                content=transaction_cat(tx, d, content)
                #Return dictionary append dictionary to list of dicts
                #Create DataFrame with appended content   !!!!!!NEED TO APPEND ALL TXS FROM DF_OUTS 
                df = pd.DataFrame(content)
                df.rename(columns={'in':'ins'}, inplace=True)
                
    #Sending the dataframe containing the collected transactions to be unwound and added to the JSON file
    CURR_DEPTH_OUTS = CURR_DEPTH_OUTS + 1
    if int(CURR_DEPTH_OUTS) > int(MAX_DEPTH):
        print("XXXXXX MAX_DEPTH EXCEEDED XXXXXX")
        print("****** CURR DEPTH = " +str(CURR_DEPTH_OUTS)+" is > MAX DEPTH = " +str(MAX_DEPTH)+" ******")
    else: 
        unwind_df_outs(df, MAX_DEPTH, CURR_DEPTH_OUTS, aSeed, path, df_outs_content, df_content)

#****************************************************************
#Support Function: export_to_JSON() takes a transaction id and appends the response from the API to the list of dictionaries collected 
#also calls the download_data_prep() module to manage the API request
#****************************************************************
def export_to_JSON(path, file_name, df):
    export = df.to_dict('records')
    with open(path+file_name+'.json', 'w') as f:
        json.dump(export, f, indent = 4, sort_keys = True)

#****************************************************************
#Support Function: download() takes a transaction id and appends the response from the API to the list of dictionaries collected 
#also calls the download_data_prep() module to manage the API request
#****************************************************************
def download(API_URL, file_name):
    with request.urlopen(API_URL) as response:
        if response.getcode() == 200:
            print('****** Calling Wallet Explorer API ******')
            source = response.read()
            data = json.loads(source)
            #return data
            #return pd.DataFrame.from_dict(json_normalize(data), orient='columns')
            with open('/Users/adamturner/Documents/MPICT/Research/9_PhD/Chapters/Chapter_5/data/'+file_name+'.json', 'w') as f:
                json.dump(data, f, indent = 4, sort_keys = True)
        else:
            print('An error occurred while attempting to retrieve data from the API.')

#****************************************************************
#Support Function: download_data_prep() takes a transaction id and appends the response from the API to the list of dictionaries collected 
#also calls the download_data_prep() module to manage the API request
#****************************************************************
def download_data_prep(API_URL):
    print('****** Access Wallet Explorer API ******')
    print('****** URL: '+API_URL+' ******')
    WAIT_PERIOD = 15
    req=Request(API_URL, headers={'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11', 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'})
    sleep(WAIT_PERIOD)
    try:
        response = request.urlopen(req)
    except urllib.error.HTTPError as e:
        print (e.read())
    else:    
    #with request.urlopen(req) as response:
     #  if response.getcode() == 200:
        source = response.read()
        data = json.loads(source)
        return data

#****************************************************************
#Step 0. download_module() calls the download_module() module to create the transaction history file
#only run once and comment out so as not to run each time network files are being created by data_prep()
#
#****************************************************************
#download_module()
      
#****************************************************************
#Step 1. data_prep() calls the data_prep() module to create the cash_in and cash_out network files in JSON
#once the transaction history file has been created data_prep() uses this history listing to create the network files
#****************************************************************
data_prep()
