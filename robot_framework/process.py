from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection
from OpenOrchestrator.database.queues import QueueElement
import os
import pandas as pd
import re
import xml.etree.ElementTree as ET
import requests
import json
from urllib.parse import quote
from requests_ntlm import HttpNtlmAuth
import robot_framework.HentFilerOpretMapper as HentFilerOpretMapper
import base64
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.backends import default_backend
# pylint: disable-next=unused-argument
def process(orchestrator_connection: OrchestratorConnection, queue_element: QueueElement | None = None) -> None:
    

    orchestrator_connection = OrchestratorConnection("Aktindsigt i personalemappe - dokumentliste", os.getenv('OpenOrchestratorSQL'),os.getenv('OpenOrchestratorKey'), None)
    GOAPILIVECRED = orchestrator_connection.get_credential("GOAktApiUser")
    GOAPILIVECRED_username = GOAPILIVECRED.username
    GOAPILIVECRED_password = GOAPILIVECRED.password
    GOAPI_URL = orchestrator_connection.get_constant('GOApiURL').value
    SharepointURL = orchestrator_connection.get_constant('AktindsigtPersonalemapperSharepointURL').value
    EncryptionKey = orchestrator_connection.get_credential('PersonalesagsEncryptionKey').value

    #Get Robot Credentials
    RobotCredentials = orchestrator_connection.get_credential("Robot365User")
    RobotUsername = RobotCredentials.username
    RobotPassword = RobotCredentials.password
    data = json.loads(queue_element.data)
    cpr_encrypted = data.get('citizen_id')

    #Herunder hentes sagsinformation

    #For decryption sensitive information
    def decrypt(b64_ciphertext: str) -> str:
        combined = base64.b64decode(b64_ciphertext)
        iv = combined[:16]
        ciphertext = combined[16:]

        backend = default_backend()
        cipher = Cipher(algorithms.AES(EncryptionKey), modes.CBC(iv), backend=backend)

        decryptor = cipher.decryptor()
        padded_plaintext = decryptor.update(ciphertext) + decryptor.finalize()

        unpadder = padding.PKCS7(128).unpadder()
        plaintext = unpadder.update(padded_plaintext) + unpadder.finalize()
        return plaintext.decode()

    #-- Initialize session
    session = requests.Session()
    session.auth = HttpNtlmAuth(GOAPILIVECRED_username, GOAPILIVECRED_password)

    #Get CPR from queue element
    #data = json.loads(decrypt(queue_element['data]))
    cpr = decrypt(cpr_encrypted)
    cpr_reformatted = f'{cpr[:6]}'+'-'f'{cpr[-4:]}'

    #-- Getting personalesagsid from cpr
    url = f"{GOAPI_URL}/_goapi/search/ExecuteModernSearch"

    payload = json.dumps({
    "QueryPageIndex":1,
    "PageSize":0,
    "QueryPhrase":f'{cpr}',
    "QueryType":"Cases",
    "TrimToOpenedCases":True,
    "ResultTypeInternalName":"6376435f-d715-48ad-8e0c-0a35d85f0d5e",
    "ResultTypeName":"Oversager",
    "SearchContentDefinitionEntryType":2,
    "ResultViewInternalName":"4b82f943-e2bb-48aa-b2b3-6ab8e7d948d6",
    "AdditionalSelectColumns":["CCMTitle","CCMEmploymentCode","CCMContactData","CCMContactDataCPR","CCMAfdeling","CCMMedarbejdernummer","CCMCaseOwner","CCMParentCase","docicon","CCMDocID","CCMCaseID"],
    "ResultTypeListNameOrType":None,
    "ResultTypeSearchOnlyItems":True,
    "ResultTypeQueryFilter":"AND -ccmparentcase:\"P*\" -ccmparentcase:\"B*\" -ccmparentcase:\"E*\"",
    "CaseQueryFieldCollection":[],
    "QueryFieldCollection":[],
    "CaseTypePrefixes":["PER"],
    "SortDirection1":1,
    "ResultViewSortField1":None,
    "ResultViewSortField2":None,
    "ResultViewSortOrder1":2,
    "ResultViewSortOrder2":2,
    "QueryScope":0})
    headers = {
    'Content-Type': 'application/json'
    }

    response = session.request("POST", url, headers=headers, data=payload)
    data = response.json()['results']['Results']
    caseurl = next((item["caseurl"] for item in data if cpr_reformatted in item.get("title", "")), None)
    PersonaleSagsID = caseurl.split('/')[-1]
    AktID = caseurl.split('/')[1]

    #-- Getting list id from personesagsid
    url = f"{GOAPI_URL}/cases/{AktID}/{PersonaleSagsID}/_goapi/cases/CaseDetailsInternal"

    response = session.request("POST", url, json = {}, timeout=30)

    data = response.json()
    ListId = data['d'].get('ListId')
    #-- få Titler fra ListID:

    url = f"{GOAPI_URL}/personalemapper/_api/web/lists(guid'{ListId}')/RenderListDataAsStream?CCMRequest=true&RootFolder=%2Fpersonalemapper%2FLists%2FCases3&FilterField1=CCMParentCase&FilterValue1={PersonaleSagsID}%3B%23PER"

    payload = json.dumps({
    "parameters": {
        "AddRequiredFields": True,
        "RenderOptions": 2,
        "ViewXml": "<View Name=\"{BC9CD5F3-B052-4940-A4A4-1CBA4CFC497E}\" Type=\"HTML\" Scope=\"Recursive\" DisplayName=\"UndersagerOpen\" Url=\"/personalemapper/Lists/Cases3/UndersagerOpen.aspx\" Level=\"1\" BaseViewID=\"1\" ContentTypeID=\"0x\" ImageUrl=\"/_layouts/15/images/generic.png?rev=23\"><ViewFields><FieldRef Name=\"CaseLinkTitle\" /><FieldRef Name=\"Afdeling\" /><FieldRef Name=\"CaseID\" /><FieldRef Name=\"ContentsLastChangedDate\" /></ViewFields><Toolbar Type=\"Standard\" /><XslLink Default=\"TRUE\">ccm.xsl</XslLink><JSLink>clienttemplates.js</JSLink><RowLimit Paged=\"TRUE\">100</RowLimit><Query><OrderBy><FieldRef Name=\"ID\" /></OrderBy></Query><ParameterBindings><ParameterBinding Name=\"NoAnnouncements\" Location=\"Resource(wss,noXinviewofY_LIST)\" /><ParameterBinding Name=\"NoAnnouncementsHowTo\" Location=\"Resource(wss,noXinviewofY_DEFAULT)\" /></ParameterBindings></View>"
    }
    })
    headers = {
    'Content-Type': 'application/json'
    }

    response = session.request("POST", url, headers=headers, data=payload)

    data = response.json()

    rows = data.get('Row')

    SagsIDListe = [element['CaseID'] for element in rows]
    MappeNavne = [element['Title'] for element in rows]


    #Filinformation hentes ud fra GO, og herudfra oprettes mapper i sharepoint
    for i in range(len(SagsIDListe)):
        HentFilerOpretMapper.HentFilerOpretMapper(PersonaleSagsID=PersonaleSagsID, SagsID= SagsIDListe[i], MappeNavn = MappeNavne[i], GOAPI_URL= GOAPI_URL, GOAPILIVECRED_username= GOAPILIVECRED_username, GOAPILIVECRED_password= GOAPILIVECRED_password, SharepointURL=SharepointURL, RobotUsername=RobotUsername, RobotPassword= RobotPassword)
        print(f'Oprettet mapper for {MappeNavne[i]}')