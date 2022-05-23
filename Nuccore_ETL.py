# -*- coding: utf-8 -*-
import pandas as pd
import re
import json
import requests
from contextlib import closing
import sqlite3

# Imprting prefect
from prefect import task, Flow, Parameter

# Importing helpers from elasticsearch
from elasticsearch import helpers
from es import elastic_upload
from utils import save_to_json
from sql_helpers import sql_connector

es_ = elastic_upload()
mycursor, mydb = sql_connector()
import mysql.connector



import xmltodict
def get_data_from_xml(response):
    print(response)
    # url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=nuccore&retmode=xml&tool=rex-ncbi&id=2230715355"
    # response = requests.get(url)
    data = xmltodict.parse(response)
    title = data['GBSet']['GBSeq']['GBSeq_definition']
    source = data['GBSet']['GBSeq']['GBSeq_source']
    organism = data['GBSet']['GBSeq']['GBSeq_organism']
    taxonomy = data['GBSet']['GBSeq']['GBSeq_taxonomy']

    res = [title, source,  organism, taxonomy]

    return res


def fetch_data_from_id(id, query):
    print('Id', id)
    response = requests.get(
        'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=nuccore&retmode=xml&tool=rex-ncbi&id={}'.format(id))
    if response.status_code == 400:
        raise RuntimeError("Invalid PMID/Search Query ({})".format(query))
    return get_data_from_xml(response.content)




@task
def fetch_data_from_query(query):
    query_response = requests.get(
            'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=nuccore&retmode=json&tool=rex-ncbi&term=%s&retmax=4' % str(query)).json()
    nuccore_list = query_response['esearchresult']['idlist']     

    all_data = []
    json_dataset = []
    ids_json = {}
    for i in nuccore_list:
        output = fetch_data_from_id(i, query)
        #print("-------------------------------------------------------")
        #print("this is ", nuccore_list)
        print(output)
        
        json_data = {}
        json_data['id'] = i
        json_data['title'] = output[0]
        json_data['source'] = output[1] 
        json_data['organism'] = output[2]
        json_data['taxonomy'] = output[3]
        ids_json[query] = nuccore_list
        json_dataset.append(json_data)
        all_data.append([i, output[0], output[1], output[2] , output[3]])
        
    #es_.index(index="id-index1", id=1, document=json.dumps(ids_json))
    #helpers.bulk(es_, json_dataset, index='nuccore-data',)
    #es_.index(index="nuccore-data", id=2, document=json.dump(json_dataset))

    store_id_data(ids_json)
    store_json_data(json_dataset)
    return all_data

@task
def store_ncbi_data(parsed):
    create_script = 'CREATE TABLE IF NOT EXISTS nuccore_table (id INT, title TEXT, source TEXT, organism TEXT, taxonomy TEXT)'
    insert_cmd = "INSERT INTO nuccore_table VALUES (?, ?, ?, ?, ?)"
    select_cmd = "SELECT * FROM nuccore_table"


    with closing(sqlite3.connect("prefect_test.db")) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.executescript(create_script)
            cursor.executemany(insert_cmd, parsed)
            a = cursor.execute(select_cmd)
            conn.commit()
	
            
            #Delete the duplicate data (deduplicate)
            cursor.execute('''DELETE FROM nuccore_table WHERE EXISTS 
            ( SELECT 1 FROM nuccore_table t2 WHERE nuccore_table.id = t2.id AND nuccore_table.rowid > t2.rowid)''')
            conn.commit()


            # Display data inserted -- test done for only 4 ids 
            print("Data Inserted in the table: ")
            data=cursor.execute('''SELECT * FROM nuccore_table''')
            for row in data:
                print(row)
        
 

def store_json_data(result):
    save_to_json('nuccore_data.json', result)


def store_id_data(result):
    save_to_json('nuccore_id_data.json', result)


with Flow("nuccore-flow") as flow:
    query = Parameter('NCBI', default='NCBI')
    parsed = fetch_data_from_query(query)
    store_ncbi_data(parsed)

flow.run()
flow.register(project_name="prefect_Assignment")


#for fasta files 
#conda install -c bioconda ncbi-genome-download
#ncbi-genome-download --formats fasta,assembly-report viral


#For docker
#built_storage = flow.storage.build(push=False)
