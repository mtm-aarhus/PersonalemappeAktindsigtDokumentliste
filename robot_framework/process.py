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
from sqlalchemy import create_engine, text
from datetime import datetime
from urllib.parse import quote_plus
import re

def process(orchestrator_connection: OrchestratorConnection, queue_element: QueueElement | None = None) -> None:

    GOAPILIVECRED = orchestrator_connection.get_credential("GOAktApiUser")
    GOAPILIVECRED_username = GOAPILIVECRED.username
    GOAPILIVECRED_password = GOAPILIVECRED.password
    GOAPI_URL = orchestrator_connection.get_constant('GOApiURL').value
    SharepointURL = orchestrator_connection.get_constant('AktindsigtPersonalemapperSharepointURL').value
    EncryptionKey = orchestrator_connection.get_credential('PersonalesagsEncryptionKey').password

    #Get Robot Credentials
    RobotCredentials = orchestrator_connection.get_credential("Robot365User")
    RobotUsername = RobotCredentials.username
    RobotPassword = RobotCredentials.password
    data = json.loads(queue_element.data)
    cpr_encrypted = data.get('citizen_id')
    caseid = data.get('caseid')
    personalesagsid = data.get('personalesagsid')

    #Herunder hentes sagsinformation
    MANUAL_CASE_REGEX = re.compile(r"^\d{6}-\d{4}$")

    def is_manual_case(case_id: str) -> bool:
        """Returnerer True hvis case_id matcher formatet for manuelt oprettede sager."""
        return bool(MANUAL_CASE_REGEX.match(case_id))

    #For decryption sensitive information
    def decrypt(b64_ciphertext: str) -> str:
        combined = base64.b64decode(b64_ciphertext)
        iv = combined[:16]
        ciphertext = combined[16:]

        key = base64.b64decode(EncryptionKey)  # tilføj denne linje
        cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())

        decryptor = cipher.decryptor()
        padded_plaintext = decryptor.update(ciphertext) + decryptor.finalize()

        unpadder = padding.PKCS7(128).unpadder()
        plaintext = unpadder.update(padded_plaintext) + unpadder.finalize()
        return plaintext.decode('utf-8')


    #-- Initialize session
    session = requests.Session()
    session.auth = HttpNtlmAuth(GOAPILIVECRED_username, GOAPILIVECRED_password)

    #Get CPR from queue element
    #data = json.loads(decrypt(queue_element['data]))
    if not is_manual_case(caseid):
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
    else:
        #-- Getting aktid from personalesagsnummer
        url = f"{GOAPI_URL}/_goapi/search/ExecuteModernSearch"

        payload = json.dumps({
        "QueryPageIndex":1,
        "PageSize":0,
        "QueryPhrase":f'{personalesagsid}',
        "QueryType":"Cases",
        "TrimToOpenedCases":False,
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
        caseurl = next((item["caseurl"] for item in data if personalesagsid in item.get("caseid", "")), None)
        AktID = caseurl.split('/')[1]
        PersonaleSagsID = personalesagsid


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
        HentFilerOpretMapper.HentFilerOpretMapper(caseid=caseid, PersonaleSagsID=PersonaleSagsID, SagsID= SagsIDListe[i], MappeNavn = MappeNavne[i], GOAPI_URL= GOAPI_URL, GOAPILIVECRED_username= GOAPILIVECRED_username, GOAPILIVECRED_password= GOAPILIVECRED_password, SharepointURL=SharepointURL, RobotUsername=RobotUsername, RobotPassword= RobotPassword, orchestrator_connection= orchestrator_connection)
        print(f'Oprettet mapper for {MappeNavne[i]}')

    overmappenavn = str(caseid) + " - " + str(PersonaleSagsID) + " - Personaleaktindsigtsanmodning"
    if len(overmappenavn) > 99:
        overmappenavn = overmappenavn[:95] + "(...)"


    SQL_SERVER = orchestrator_connection.get_constant('SqlServer').value 
    DATABASE_NAME = "AktindsigterPersonalemapper"

    odbc_str = (
        "DRIVER={SQL Server};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={DATABASE_NAME};"
        "Trusted_Connection=yes;"
    )

    odbc_str_quoted = quote_plus(odbc_str)
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={odbc_str_quoted}", future=True)


    dokumentliste_link = (
        f"{SharepointURL}/Delte dokumenter/Dokumentlister/"
        f"{caseid} - {PersonaleSagsID} - Personaleaktindsigtsanmodning"
    )

    sql = text("""
        UPDATE dbo.cases
        SET Dokumentlistemappelink = :link,
            last_run_accepted = :ts,
            documentlistfolder = :overmappenavn
        WHERE aktid = :caseid
    """)

    with engine.begin() as conn:
        result = conn.execute(sql, {
            "link": dokumentliste_link,
            "ts": datetime.now(),
            "overmappenavn": overmappenavn,
            "caseid": str(caseid)
        })
        if result.rowcount == 0:
            orchestrator_connection.log_info(f"⚠️ Ingen sag fundet med aktid={caseid}")
        else:
            orchestrator_connection.log_info(f"✅ Opdateret sag {caseid} med link:")

