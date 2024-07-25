from io import StringIO
from xml.etree.ElementTree import XML, XMLParser
import azure.functions as func
import logging
import os
from sqlalchemy import create_engine, Engine, text
import pandas as pd
from bs4 import BeautifulSoup
import requests
from azure.storage.blob import BlobServiceClient, ContainerClient, PublicAccess
from dataclasses import dataclass, field
from typing import Optional, Dict

from sqlalchemy.sql.base import Options
from sqlalchemy.sql.lambdas import NonAnalyzedFunction

app = func.FunctionApp()


logger = logging.getLogger(__name__)
DB_NAME = os.getenv("DB_NAME") or "runiddb"
DB_HOST = os.getenv("DB_HOST") or "localhost"
DB_USER = os.getenv("DB_USER") or "emanuel"
DB_PASSWORD = os.getenv("DB_PASSWORD") or "superpassword"
DB_PORT = os.getenv("DB_PORT") or 5432

BASE_URLS = {
    "staging": "https://pasta-s.lternet.edu/",
    "development": "https://pasta-d.lternet.edu/",
    "production": "https://pasta.lternet.edu/",
}

EML_PATHS = {
    "dataset_name": "eml.dataset.datatable.entityName",
    "dataset_description": "eml.dataset.datatable.entityDescription",
    "csv_url": "eml.dataset.datatable.physical.distribution.online.url",
    "csv_size": "eml.dataset.dataTable.physical.size",
}


@dataclass
class EDIPipe:
    pkg_number: str
    az_blob_conn_str: str = field(repr=False)
    db_connection_string: str = field(repr=False)
    container_client: ContainerClient | None = None
    db_engine: Engine | None = None


def initialize_pipe(pipe: EDIPipe):
    """
    Initialize a pipe by checking if a corresponding blob structure exists, if not create it.
    Initialization also creates a connection to the database given the connection string. The
    fields `db_engine` and `container_client` are both populated after init is complete, for use
    in other functions.
    """
    # create blob connection
    blob_service_client = BlobServiceClient.from_connection_string(
        pipe.az_blob_conn_str
    )
    container_client = blob_service_client.get_container_client(pipe.pkg_number)
    if not container_client.exists():
        logging.info("container not found, creating from template...")
        container_client.create_container(public_access=PublicAccess.CONTAINER)
        init_files = ["xml/init.txt", "data/init.txt"]
        for file in init_files:
            container_client.upload_blob(file, b"")
        logging.info("creation successful")
    # create db connection
    db_engine = create_engine(pipe.db_connection_string)
    pipe.container_client = container_client
    pipe.db_engine = db_engine


def read_sql_from_file(file_name: str):
    with open(file_name, "r") as f:
        return f.read()


def get_latest_data(db: Optional[Engine], query_statement: str):
    if db is None:
        raise Exception(
            "pipe db set to None, please initialize pipe with `initialize_pipe`"
        )
    else:
        with db.connect() as conn:
            data = pd.read_sql_query(query_statement, conn)
        return data


def upload_csv_to_blob(blob_prefix, blob_service_client, filename, data):
    blob_client = blob_service_client.get_blob_client(
        container=blob_prefix, blob=f"data/{filename}.csv"
    )
    csv_binary = StringIO()
    data.to_csv(csv_binary, index=False)
    csv_content = csv_binary.getvalue()
    blob_client.upload_blob(csv_content)
    return blob_client.url


def get_package_xmls(blob_service_client, sort=True):
    blob_list = list(blob_service_client.list_blobs(name_starts_with="xml/"))
    if len(blob_list) == 0:
        return None
    if sort:
        return sorted(blob_list, key=lambda x: x["last_modified"], reverse=True)
    return blob_list


def get_url_for_xml(blob_name, blob_service_client):
    blob_client = blob_service_client.get_blob_client(blob_name)
    return blob_client.url


def parse_xml_from_url(url: str):
    resp = requests.get(url)
    content = resp.content
    soup = BeautifulSoup(content, "lxml-xml")
    return soup


def update_package_id(xml, new_id):
    eml_tag = xml.find("eml:eml")
    if not eml_tag:
        raise Exception("unable to locate top level eml tag in xml file")

    eml_tag["packageId"] = new_id


def update_eml(eml: BeautifulSoup, kv: Dict[str, str]):
    for path, val in kv.items():
        node_path = path.split(".")
        current = eml
        for node in node_path:
            current = current.find(node)
            if current is None:
                break

        if current is not None:
            current.clear()
            current.append(val)


def package_id_revision_increment(self):
    current_package_id = self.get_package_id()
    if current_package_id:
        package_id_parts = current_package_id.split(".")
        new_revision_number = int(package_id_parts[2]) + 1
        return f"{package_id_parts[0]}.{package_id_parts[1]}.{new_revision_number}"


def update_package_id_tag(self):
    new_package_id = self.package_id_revision_increment()
    eml_tag = self.soup.find("eml:eml")
    if eml_tag:
        eml_tag["packageId"] = new_package_id


def write_xml_to_blob(
    xml: BeautifulSoup, package_number: str, container_client: ContainerClient
):
    eml_tag = xml.find("eml:eml")
    package_id = eml_tag["packageId"]
    filename = f"xml/{package_id}.xml"
    blob_client = container_client.get_blob_client(
        container=package_number, blob=filename
    )
    xml_content = str(xml)
    blob_client.upload_blob(xml_content, overwrite=True)


# set up the container and azure blob

# get latest data


@app.function_name(name="genetics-EDI-processing")
@app.route(route="fetch-data")
def main(req: func.HttpRequest) -> func.HttpResponse:
    az_conn_string = os.environ["AZURE_BLOB_CONN_STRING"]
    db_conn_string = os.environ["DB_CONN_STRING"]
    pipe = EDIPipe("edi-1616", az_conn_string, db_conn_string)
    initialize_pipe(pipe)

    q = read_sql_from_file("data-query.sql")
    data = get_latest_data(pipe.db_engine, q)
    new_url = upload_csv_to_blob(
        pipe.pkg_number, pipe.container_client, "genetics-data", data
    )

    xmls = get_package_xmls(pipe.container_client, sort=True)
    xml_url = get_url_for_xml(xmls[0].name)
    xml_soup = parse_xml_from_url(xml_url)

    update_eml(xml_soup, {EML_PATHS["csv_url"]: new_url})

    write_xml_to_blob(xml_soup, pipe.container_client)

    return func.HttpResponse("EDI Pipeline Excecution Complete")


# class EDIPipeline:
#     """
#     EDI Pipeline
#     - xml_container: the blob container where metadata xml files are stored
#     - server: the edi server to use for data uploads
#     """
#
#     def __init__(
#         self,
#         az_blob_container: str,
#         server="staging",
#     ):
#         self.az_blob_container = az_blob_container
#         self._base_url = BASE_URLS.get(server)
#         self.query_file = "data-query.sql"
#
#         # self._get_latest_xml_url()
#         # self._make_soup()
#
#     def az_blob_connect(self, conn_str: str):
#         self.blob_service_client = BlobServiceClient.from_connection_string(conn_str)
#         container_client = self.blob_service_client.get_container_client(
#             self.az_blob_container
#         )
#
#         # create bucket if it doesn't exist alredy and add temp folders in each
#         if not container_client.exists():
#             logging.info("container not found, creating from template...")
#             container_client.create_container(public_access=PublicAccess.CONTAINER)
#             init_files = ["xml/init.txt", "data/init.txt"]
#             for file in init_files:
#                 container_client.upload_blob(file, b"")
#             logging.info("creation succesfull")
#
#         self.container_client = container_client
#
#     def db_connect(
#         self,
#         dbname=os.getenv("DB_NAME"),
#         user=os.getenv("DB_USER"),
#         password=os.getenv("DB_PASSWORD"),
#         host=os.getenv("DB_HOST"),
#         port=os.getenv("DB_PORT"),
#     ):
#         db_conn_string = (
#             f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
#         )
#         self.engine = create_engine(db_conn_string)
#
#     def _get_package_xmls(self):
#         # TODO: get this from the env/az app settings
#         blob_list = list(self.container_client.list_blobs(name_starts_with="xml/"))
#
#         if len(blob_list) == 0:
#             return None
#
#         return sorted(blob_list, key=lambda x: x["last_modified"], reverse=True)
#
#     def _get_latest_xml_url(self):
#         all_xmls = self._get_package_xmls()
#         if all_xmls:
#             latest = all_xmls[0]
#             blob_client = self.container_client.get_blob_client(latest.name)
#             self.latest_url = blob_client.url
#             return blob_client.url
#         else:
#             return None
#
#     def _make_soup(self):
#         resp = requests.get(self.latest_url)
#         content = resp.content
#         soup = BeautifulSoup(content, "lxml-xml")
#         self.soup = soup
#
#     def get_package_id(self):
#         eml_tag = self.soup.find("eml:eml")
#         if eml_tag is not None:
#             return eml_tag["packageId"]
#
#     def package_id_revision_increment(self):
#         current_package_id = self.get_package_id()
#         if current_package_id:
#             package_id_parts = current_package_id.split(".")
#             new_revision_number = int(package_id_parts[2]) + 1
#             return f"{package_id_parts[0]}.{package_id_parts[1]}.{new_revision_number}"
#
#     def update_package_id_tag(self):
#         new_package_id = self.package_id_revision_increment()
#         eml_tag = self.soup.find("eml:eml")
#         if eml_tag:
#             eml_tag["packageId"] = new_package_id
#
#     def write_xml_to_blob(self):
#         current_id = self.get_package_id()
#         filename = f"xml/{current_id}.xml"
#         blob_client = self.blob_service_client.get_blob_client(
#             container=self.az_blob_container, blob=filename
#         )
#         xml_content = str(self.soup)
#         blob_client.upload_blob(xml_content, overwrite=True)
#
#     def write_csv_to_blob(self, filename: str, data: pd.DataFrame):
#         blob_client = self.blob_service_client.get_blob_client(
#             container=self.az_blob_container, blob=f"data/{filename}.csv"
#         )
#         csv_binary = StringIO()
#         data.to_csv(csv_binary, index=False)
#         csv_content = csv_binary.getvalue()
#
#         blob_client.upload_blob(csv_content)
#
#         return blob_client.url
#
#     def read_sql_file(self, file_name: str):
#         with open(file_name, "r") as f:
#             return f.read()
#
#     def set_query_file(self, file_name: str):
#         self.query_file = file_name
#
#     def get_latest_data(self):
#         query_statement = self.read_sql_file(file_name=self.query_file)
#
#         with self.engine.connect() as conn:
#             data = pd.read_sql_query(query_statement, conn)
#         return data
#
#     def latest_xml_to_blob(self):
#         self.update_package_id_tag()
#         self.write_xml_to_blob()
#
#     def update_package_metadata(
#         self, base_xml_url: str, overwrite_values: dict[str, str]
#     ):
#         self._get_latest_xml_url()
#         self._make_soup()
#
#         overwrite_values = overwrite_values
#
