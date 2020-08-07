import os
import requests
import json
import xlrd
import auth_api_token
import endpoints

datataprep_auth_token = auth_api_token.datataprep_auth_token
dataprep_headers = auth_api_token.dataprep_headers


def readconfig(sheetname):
    newdict = dict()
    loc = "config.xlsx"
    wb = xlrd.open_workbook(loc)
    sheet = wb.sheet_by_name(sheetname)
    for i in range(sheet.nrows):
        newdict[sheet.cell_value(i, 0)] = sheet.cell_value(i, 1)
    print(newdict)
    return newdict


def execute_post(url, headers, data):
    resp = requests.post(
        url=url,
        headers=headers,
        data=json.dumps(data),
        )
    print('Status Code : {}'.format(resp.status_code))
    print('Result : {}'.format(resp.json()))

    return resp


def dataprep_flow_create():
    print('Creating the flow ... ')
    dataprep_runjob_endpoint = endpoints.create_flow_endpoint
    config = readconfig('Flow')
    datataprep_job_param = {
        "name": config['FlowName'],
        "description": config["Description"]
        }
    print('Run Dataprep job param: {}'.format(datataprep_job_param))
    print('Run Dataprep header: {}'.format(dataprep_headers))
    resp = execute_post(
        url=dataprep_runjob_endpoint,
        headers=dataprep_headers,
        data=datataprep_job_param,
        )

    flowid = resp.json()['id']
    return flowid


def dataprep_import_dataset_create(index, config):
    print('Creating the import Dataset ... ')
    dataprep_runjob_endpoint = endpoints.create_import_dataset_endpoint

    datataprep_job_param = {
        "uri": config['uri' + index],
        "name": config['DatasetName' + index],
        "description": config['Description' + index]
        }
    print('Run Dataprep job param: {}'.format(datataprep_job_param))
    print('Run Dataprep header: {}'.format(dataprep_headers))
    resp = execute_post(
        url=dataprep_runjob_endpoint,
        headers=dataprep_headers,
        data=datataprep_job_param,
        )
    datasetid = resp.json()['id']
    RecipeName = config['RecipeName' + index]

    return datasetid, RecipeName


def dataprep_recipe_create(datasetid, flowId, RecipeName):
    dataprep_runjob_endpoint = endpoints.create_recipe_endpoint
    print('Creating the Recipe ... ')
    datataprep_job_param = {"name": RecipeName,
                            "importedDataset": {
                                "id": datasetid
                                },
                            "flow": {
                                "id": flowId
                                }
                            }
    print('Run Dataprep job param: {}'.format(datataprep_job_param))
    print('Run Dataprep header: {}'.format(dataprep_headers))
    resp = execute_post(
        url=dataprep_runjob_endpoint,
        headers=dataprep_headers,
        data=datataprep_job_param,
        )

    recipeid = resp.json()['id']
    return recipeid


def dataprep_output_create(recipe_id):
    dataprep_runjob_endpoint = endpoints.create_output_object_endpoint
    config = readconfig('Output')
    datataprep_job_param = {
        "execution": config['execution'],
        "profiler": config['profiler'],
        "isAdhoc": config['isAdhoc'],
        "writeSettings": {
            "data": [
                {
                    "delim": config['delim'],
                    "path": config['path'],
                    "action": config['action'],
                    "format": config['format'],
                    "compression": config['compression'],
                    "header": config['header'],
                    "asSingleFile": config['asSingleFile'],
                    "prefix": config['prefix'],
                    "suffix": config['suffix'],
                    "hasQuotes": config['hasQuotes']
                    }
                ]
            },
        "flowNode": {
            "id": recipe_id
            }
        }
    print('Run Dataprep job param: {}'.format(datataprep_job_param))
    print('Run Dataprep header: {}'.format(dataprep_headers))
    resp = execute_post(
        url=dataprep_runjob_endpoint,
        headers=dataprep_headers,
        data=datataprep_job_param,
        )
    outputid = resp.json()['id']
    return outputid

def dataprep_publication_create(outputObjectId):
    dataprep_runjob_endpoint = endpoints.create_recipe_endpoint
    print('Connect with the  Publication... ')
    config = readconfig('Publish')
    datataprep_job_param = {
        "path": [
            config["path"]
            ],
        "tableName": config["tableName"],
        "targetType": config["targetType"],
        "action": config["action"],
        "outputObject": {
            "id": outputObjectId
            },
        "connection": {
            "id": config["connectionId"]
            }
        }
    print('Run Dataprep job param: {}'.format(datataprep_job_param))
    print('Run Dataprep header: {}'.format(dataprep_headers))
    resp = execute_post(
        url=dataprep_runjob_endpoint,
        headers=dataprep_headers,
        data=datataprep_job_param,
        )

    publicationid = resp.json()['id']
    return publicationid


def start_dataprep_process():
    """ Create Flow"""
    flowid = dataprep_flow_create()

    config = readconfig('Dataset_Recipe')
    sourcecount = config['Source_Count']

    for i in range(1, int(sourcecount) + 1):
        index = str(i)

        """ Create Imported Dataset """
        datasetid, RecipeName = dataprep_import_dataset_create(index, config)
        """ Create Recipe """
        recipeid = dataprep_recipe_create(datasetid, flowid, RecipeName)
        """ Create Output """
        outputObjectId = dataprep_output_create(recipeid)
        """ Create oublication """
        publicationid = dataprep_publication_create(outputObjectId)


start_dataprep_process()
