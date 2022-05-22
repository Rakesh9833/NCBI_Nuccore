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



def store_json_data(result):
    save_to_json('nuccore_data.json', result)


def store_id_data(result):
    save_to_json('nuccore_id_data.json', result)



@task
def fetch_data_from_query(query):
    query_response = requests.get(
            'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=nuccore&retmode=json&tool=rex-ncbi&term=%s&retmax=100' % str(query)).json()
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
    # helpers.bulk(es_, json_dataset, index='gds-data',)
    # # es_.index(index="gds-data", id=2, document=json.dump(json_dataset))
    store_id_data(ids_json)
    store_json_data(json_dataset)
    return all_data

@task
def store_ncbi_data(parsed):
    create_script = 'CREATE TABLE IF NOT EXISTS nuccore_table (id INT, title TEXT, source TEXT, organism TEXT, taxonomy TEXT)'
    insert_cmd = "INSERT INTO gds_table VALUES (?, ?, ?, ?, ?)"

    with closing(sqlite3.connect("nuccore_data.db")) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.executescript(create_script)
            cursor.executemany(insert_cmd, parsed)
            conn.commit()



with Flow("nuccore-flow") as flow:
    query = Parameter('NCBI', default='NCBI')
    parsed = fetch_data_from_query(query)
    # store_ncbi_data(parsed)

flow.run()
flow.register(project_name="prefect_Assignment")

#For docker
#built_storage = flow.storage.build(push=False)
