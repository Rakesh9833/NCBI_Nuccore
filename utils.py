import json

def save_to_json(filename, json_dataset):
    with open(filename, 'w') as outfile:
        json.dump(json_dataset, outfile)