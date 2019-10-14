# -*- coding: utf-8 -*-
"""
Description: Autonomous Database Data uploader from different kind files
     Author: shuaijun.yang@oracle.com
       Date: Oct 10, 2010
     Python: 3.7
   Packages: cx_Oracle, sqlalchemy, pandas, xlrd, rsa
"""
import cx_Oracle
import sqlalchemy
import pandas as pd
import os
import json 
import argparse
import re
import sys
import rsa
import base64
import getpass


def preCheck():
    """
    Check whether there is user configuration json files and client.
    """

    # check the existence of rsa key 
    homeDir=os.environ['HOME']

    if not os.path.exists(homeDir+'/.adb'):
        os.mkdir(homeDir+'/.adb')

    if not os.path.exists(homeDir+'/.adb/private.pem'):
        genKey()

    # Check the existence of TNS_ADMIN
    if os.environ.get('ORACLE_HOME') :
        if os.environ.get('TNS_ADMIN') :
            # Check the existence of the configuration file.
            if not os.path.exists(homeDir+"/.adb/config.json") :
                configADB()
        else:
            print("""
First, download the Autonomous Database Wallet from OCI into the instant client directory.
Secondly, set the environment variable TNS_ADMIN.
Third, modify the sqlnet.ora to contain the correct wallet file path.
Finally, run this program!
                  """)
            exit(-1)
    else:
        print("""
First, download the instant client driver from:
   https://www.oracle.com/database/technologies/instant-client/downloads.html
Secondly, set the environment variable ORACLE_HOME.
Finally, run this program!
          """)
        exit(-1)
    
def configADB():
    """
    Configure the Autonomous Database Connection
    """
    # check the .adb dir existence
    homeDir=os.environ['HOME']

    pubFile=open(homeDir+'/.adb/public.pem','rb')
    rawData=pubFile.read()
    pubFile.close()
    pubKey=rsa.PublicKey.load_pkcs1(rawData,format='PEM')

    adbDir=homeDir+'/.adb'
    cfg={}
    cfg['user']=input("Enter your ADB username:")
    passwd_str=getpass.getpass("Enter your ADB user's password:")
    cfg['passwd']=base64.encodebytes(rsa.encrypt(passwd_str.encode('utf8'),pubKey)).decode('ascii')
    cfg['TNS']=input("Enter your ADB TNS's name:")

    cfgFile=open(adbDir+"/config.json",'w')
    json.dump(cfg,cfgFile)
    cfgFile.close()

def genKey():
    """
    Generate the RSA key to encrypt the user password
    """
    # check the .adb dir existence
    homeDir=os.environ['HOME']

    (pubkey, privkey) = rsa.newkeys(2048)
    privFile=open(homeDir+'/.adb/private.pem','wb')
    privFile.write(privkey.save_pkcs1(format='PEM'))
    privFile.close()

    pubFile=open(homeDir+'/.adb/public.pem','wb')
    pubFile.write(pubkey.save_pkcs1(format='PEM'))
    pubFile.close()

    
def adbConnect():
    """
    Establish the database connection
    """
    homeDir=os.environ['HOME']
    cfgFile=open(homeDir+"/.adb/config.json",'r')
    cfg={}
    cfg=json.load(cfgFile)
    cfgFile.close()

    # decrypt the passwd
    privFile=open(homeDir+"/.adb/private.pem",'rb')
    rawData=privFile.read()
    privFile.close()
    privKey=rsa.PrivateKey.load_pkcs1(rawData,format='PEM')

    passwd=rsa.decrypt(base64.decodebytes(cfg['passwd'].encode('ascii')),privKey).decode('utf8')
    
    os.environ['NLS_LANG']="SIMPLIFIED CHINESE_CHINA.UTF8"
    connstr= "oracle+cx_oracle://{}:{}@{}".format(cfg['user'],passwd,cfg['TNS'])
    return sqlalchemy.create_engine(connstr, max_identifier_length=128)

def loadCSV(conn, destSchema, destTable, dataType, srcFile):
    """
    Load csv data into ADB
    """
    df=pd.read_csv(srcFile,encoding='utf-8')
    df.to_sql(destTable, conn, schema=destSchema, if_exists='append', index=False, chunksize=10000, dtype=dataType)
    
def loadJSON(conn, destSchema, destTable, dataType, srcFile):
    """
    Load JSON data into ADB
    """
    df=pd.read_json(srcFile)
    df.to_sql(destTable, conn, schema=destSchema, if_exists='append', index=False, chunksize=10000, dtype=dataType)
    
def loadExcel(conn, destSchema, destTable, dataType, srcFile):
    """
    Load Excel data into ADB
    """
    df=pd.read_excel(srcFile)
    df.to_sql(destTable, conn, schema=destSchema, if_exists='append', index=False, chunksize=10000, dtype=dataType)
    

if __name__ == '__main__':
    preCheck()
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--schema", help="Autonomous Database's schema")
    parser.add_argument("-t", "--table", help="Schema's table")
    parser.add_argument("-d", "--srcFile", help="Source data file to been loaded")
    args = vars(parser.parse_args())

    if len(sys.argv) == 1:
        parser.print_help()
        exit(-1)

    
    adbConn=adbConnect()
    
    # Generate the dtype
    colType={}
    metadata=sqlalchemy.MetaData(schema=args['schema'])
    tabDef=sqlalchemy.Table(args['table'], metadata, autoload=True, autoload_with=adbConn)
    for col in tabDef.columns:
        colType[col.name]=col.type
    
    if re.search("\.csv",args['srcFile']) :
        loadCSV(adbConn, args['schema'], args['table'], colType, args['srcFile'])
    
    if re.search("\.json",args['srcFile']) :
        loadJSON(adbConn, args['schema'], args['table'], colType, args['srcFile'])
    
    if re.search("\.(xls|xlsx)",args['srcFile']) :
        loadExcel(adbConn, args['schema'], args['table'], colType, args['srcFile'])
    
    # Close the connection    
    adbConn.dispose()