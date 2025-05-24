# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "bs4",
#     "requests",
# ]
# ///

import json
import copy

from pathlib import Path
from urllib.parse import urljoin
from pprint import pprint

import requests
from bs4 import BeautifulSoup, NavigableString, Tag

data_dir = Path('data')
data_dir.mkdir(exist_ok=True)

base_url = 'https://data.rbi.org.in/DBIE/'
service_base_url = 'https://data.rbi.org.in/CIMS_Gateway_DBIE/GATEWAY/SERVICES/'

def get_tag_contents(node):
    if type(node) is NavigableString:
        return '%s' % node 

    retval = [] 
    for content in node.contents:
        if type(content) is NavigableString:
            retval.append(content)
        elif type(content) is Tag and content.name not in ['style', 'script']:
            if content.name not in ['span']:
                retval.append(' ')
            retval.append(get_tag_contents(content))

    return ''.join(retval)


token = None
def service_call(session, service, body):
    global token
    service_url = urljoin(service_base_url, service)
    headers={
        'datatype': 'application/json',
        'channelkey': 'key2',
    }
    if token is not None:
        headers['authorization'] = token

    resp = session.post(service_url, json={ 'body': body }, headers=headers)
    if not resp.ok:
        raise Exception(f'Unable to complete request to {service}')

    if token is None:
        token = resp.headers['authorization']

    d = BeautifulSoup(resp.text, 'html.parser')
    txt = get_tag_contents(d)
    data = json.loads(txt)
    data_header = data['header']
    status = data_header['status']
    if status != 'success':
        pprint(data)
        raise Exception(f'Request to {service} failed') 
    return data.get('body', None)


def get_state_map(session):
    state_map_file = data_dir / 'state_map.json'
    if state_map_file.exists():
        return json.loads(state_map_file.read_text())

    state_map = {}
    state_and_dist_data = service_call(session, 'dbie_getStateAndDistrict', {})
    for sitem in state_and_dist_data['response']:
        k = sitem['state']
        ditems = sitem['subtitle']
        v = [ d['district'] for d in ditems ]
        state_map[k] = v

    state_map_file.write_text(json.dumps(state_map))

    return state_map


def get_bank_groups(session): 
    bank_group_file = data_dir / 'bank_group.json'
    if bank_group_file.exists():
        return json.loads(bank_group_file.read_text())

    bank_group_data = service_call(session, 'dbie_getBankANDBankGrp', {})

    bank_data = {}
    group_items = bank_group_data['BankGroupANDBAnkList']
    for gitem in group_items:
        v = gitem['bankGroupName']
        bitems = gitem['subtitle']
        for b in bitems:
            k = b['bankName']
            if k in bank_data:
                print(f'{k} already in mp')
            bank_data[k] = v

    bank_group_file.write_text(json.dumps(bank_data))

    return bank_data

def get_type(session, base_body, typ):
    typ_file = data_dir / f'{typ}.jsonl'
    body = copy.deepcopy(base_body)
    body['branchLocatorResultVO']['typeList'] = [ typ ]

    all_typ_data = []
    if typ_file.exists():
        with open(typ_file, 'r') as f:
            for line in f:
                item = json.loads(line)
                all_typ_data.append(item)

    offset = len(all_typ_data)
    limit = 1000
    while True:
        print(f'getting {typ} with {offset=}')
        body['offsetValue'] = offset
        body['limitValue'] = limit
        typ_data = service_call(session, 'dbie_getBankGetData', body)
        typ_data = typ_data['response']
        with open(typ_file, 'a') as f:
            for item in typ_data:
                #all_typ_data.append(item)
                f.write(json.dumps(item))
                f.write('\n')
        if len(typ_data) < limit:
            break
        offset += limit

    return all_typ_data


def main():
    session = requests.session()
    resp = session.get(base_url)
    if not resp.ok:
        raise Exception('Unable to get main page')

    service_call(session, 'security_generateSessionToken', {})


    state_map = get_state_map(session)
    pprint(state_map)

    bank_map = get_bank_groups(session)
    pprint(bank_map)

    bank_groups = list(set(bank_map.values()))

    #popgroup_data = service_call(session, 'dbie_getPopulationGroup', {})
    #pprint(popgroup_data)

    base_body = {
        "branchLocatorResultVO": {
            "districtList": [],
            "subDistrictList": [],
            "address": "",
            "typeList": [],
            "bankList": [],
            "part1Code": "",
            "stateList": list(state_map.keys()),
            "bankGroupList": bank_groups,
            "branch": "",
            "centerList": [],
            "populationGroupList": [],
            "statusType": "",
            "subTypeList": []
        },
        "offsetValue": 0,
        "limitValue": 100
    }
    pprint(base_body)

    for typ in ["BRANCH", "BC", "CSP", "OFFICE", "DBU" ]: 
        get_type(session, base_body, typ)
    #{"body":{"branchLocatorResultVO":{"districtList":[],"subDistrictList":[],"address":"","typeList":["BRANCH"],"bankList":["BANK OF BARODA","BANK OF INDIA","BANK OF MAHARASHTRA","CANARA BANK","CENTRAL BANK OF INDIA","INDIAN BANK","INDIAN OVERSEAS BANK","PUNJAB AND SIND BANK","PUNJAB NATIONAL BANK","STATE BANK OF INDIA","UCO BANK","UNION BANK OF INDIA"],"part1Code":"","stateList":["MAHARASHTRA"],"bankGroupList":["PUBLIC SECTOR BANKS"],"branch":"","centerList":[],"populationGroupList":[],"statusType":"","subTypeList":[]},"offsetValue":0,"limitValue":100}}


if  __name__ == '__main__':
    main()

