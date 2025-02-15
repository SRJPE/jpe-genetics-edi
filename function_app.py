import enum
from io import StringIO, BytesIO
import azure.functions as func
import logging
import os
from sqlalchemy import all_, create_engine, Engine
import pandas as pd
from bs4 import BeautifulSoup
import requests
from azure.storage.blob import (
    BlobServiceClient,
    ContainerClient,
    PublicAccess,
)
from dataclasses import dataclass, field
from typing import Optional, Dict
import json
import psycopg2
from psycopg2 import sql


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


def upload_csv_to_blob(
    blob_prefix, blob_service_client, filename, data, overwrite=False
):
    blob_client = blob_service_client.get_blob_client(blob=f"data/{filename}.csv")
    csv_binary = StringIO()
    data.to_csv(csv_binary, index=False)
    csv_content = csv_binary.getvalue()
    blob_client.upload_blob(csv_content, overwrite=overwrite)
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


def update_package_id_tag(self) -> None:
    new_package_id = self.package_id_revision_increment()
    eml_tag = self.soup.find("eml:eml")
    if eml_tag:
        eml_tag["packageId"] = new_package_id


def increment_package_revision_number(id: str) -> str:
    split_id = id.split(".")
    revision = int(split_id[-1]) + 1
    split_id[-1] = str(revision)
    return ".".join(split_id)


def write_xml_to_blob(xml: BeautifulSoup, container_client: ContainerClient) -> str:
    eml_tag = xml.find("eml:eml")
    if eml_tag is None:
        return None
    package_id = eml_tag.get("packageId")
    filename = f"xml/{package_id}.xml"
    blob_client = container_client.get_blob_client(filename)
    xml_content = str(xml)
    blob_client.upload_blob(xml_content, overwrite=True)

    return get_url_for_xml(filename, container_client)


app = func.FunctionApp()


def az_get_latest_xml_handle(container_client):
    all_blobs = list(container_client.list_blobs(name_starts_with="xml/"))
    if len(all_blobs):
        all_xmls = [x for x in all_blobs if x.get("name").endswith("xml")]
        sorted_xmls = sorted(all_xmls, key=lambda x: x.get("name"), reverse=True)
        blob_handle = container_client.get_blob_client(sorted_xmls[0])
        return blob_handle
    return None


def az_get_queries_handles(container_client):
    all_query_files = list(container_client.list_blobs(name_starts_with="queries/"))
    all_query_names = [x.get("name") for x in all_query_files]
    all_handles = [container_client.get_blob_client(x) for x in all_query_names]
    return all_handles


def read_query_from_handle(handle):
    handle_data = handle.download_blob()
    handle_content = handle_data.content_as_text().strip()
    return handle_content


def xml_update_package_id(soup, package_id):
    eml = soup.find("eml:eml")
    if eml is None:
        raise ValueError("could not find eml root node")
    eml["packageId"] = package_id
    return soup


def xml_get_colnames(soup):
    dts = soup.find_all("dataTable")
    entity_names = [x.find("entityName").string for x in dts]
    entity_names = [x.replace(".csv", "") for x in entity_names]
    out = {}
    for i, d in enumerate(dts):
        out[entity_names[i]] = [x.string for x in d.find_all("attributeName")]
    return out


def pasta_get_latest_revision(id: str, scope: str = "edi"):
    url = f"https://pasta.lternet.edu/package/eml/{scope}/{id}"
    resp = requests.get(url)
    if resp.status_code == 200:
        all_revisions = [
            int(revision.strip())
            for revision in resp.text.split("\n")
            if revision.strip()
        ]
        return max(all_revisions)


@app.function_name(name="edi-publish")
@app.route(route="publish", methods=["GET", "POST"])
def main(req: func.HttpRequest) -> func.HttpResponse:
    package_number = req.params.get("package_number")
    if package_number is None:
        return func.HttpResponse(
            "the package id number is not valid.\n", status_code=400
        )
    az_conn_string = os.environ["AZURE_BLOB_CONN_STRING"]
    db_conn_string = os.environ["DB_CONN_STRING"]
    pipe = EDIPipe(package_number, az_conn_string, db_conn_string)
    initialize_pipe(pipe)

    q = read_sql_from_file("data-query.sql")
    data = get_latest_data(pipe.db_engine, q)
    new_url = upload_csv_to_blob(
        pipe.pkg_number, pipe.container_client, "genetics-data", data, overwrite=True
    )

    xmls = get_package_xmls(pipe.container_client, sort=True)
    xml_url = get_url_for_xml(xmls[0].name, pipe.container_client)
    xml_soup = parse_xml_from_url(xml_url)

    return func.HttpResponse("EDI Pipeline Excecution Complete\n")


@app.function_name(name="edi-update")
@app.route(route="update", methods=["POST"])
def update(req: func.HttpRequest) -> func.HttpResponse:
    try:
        data = req.get_json()
    except ValueError:
        return func.HttpResponse("invalid input", status_code=400)

    package_id = data.get("package_id")
    if package_id is None:
        return func.HttpResponse("package_id is required", status_code=400)

    conn_str = data.get("blob_conn_string")
    if conn_str is None:
        return func.HttpResponse("blob connection string is required", status_code=400)

    db_conn_str = data.get("db_conn_string")
    if db_conn_str is None:
        return func.HttpResponse(
            "database connection string is required", status_code=400
        )

    package_name = f"edi-package-{package_id}"
    blob_cl = BlobServiceClient.from_connection_string(conn_str)
    container_cl = blob_cl.get_container_client(package_name)
    latest_xml = az_get_latest_xml_handle(container_cl)
    xml_content = latest_xml.download_blob().readall()
    soup = BeautifulSoup(xml_content.decode("utf-8"))
    local_package_revision = int(soup.find("eml:eml")["packageId"].split("\n")[-1])
    latest_remote_revision = pasta_get_latest_revision(package_id)

    # if the blob revision number before update does not match the remote one
    # then we have an issue to be resolved before we continue therefore we stop
    # here and allow user to fix manually
    if local_package_revision != latest_remote_revision:
        raise ValueError(
            f"the revision currently at blob = {local_package_revision} does not match the remote revision = {latest_remote_revision}"
        )

    # get all queries
    query_handles = az_get_queries_handles(container_cl)
    all_query_blob_names = [x.blob_name.split("/")[1] for x in query_handles]
    dataset_names = [x.replace(".sql", "") for x in all_query_blob_names]

    q_content = [read_query_from_handle(i) for i in query_handles]
    datatables_in_xml = soup.find_all("dataTable")

    # make sure datasets line up
    if len(datatables_in_xml) != len(dataset_names):
        raise ValueError(
            f"the xml defines {len(datatables_in_xml)} datables but blob xml has {len(dataset_names)}, these need tom match"
        )

    all_data_results = {}
    with psycopg2.connect(db_conn_str) as conn:
        with conn.cursor() as cur:
            for i, q in enumerate(q_content):
                cur.execute(q)
                d = cur.fetchall()
                df = pd.DataFrame(d)
                all_data_results[dataset_names[i]] = df

    # get and set the colnames for each of the datasets
    datsets_colnames = xml_get_colnames(soup)
    for k, v in all_data_results.items():
        v.columns = datsets_colnames[k]

    # write datasets to blob
    file_urls = {}
    for k, v in all_data_results.items():
        buffer = BytesIO()
        v.to_csv(buffer, index=False)
        buffer.seek(0)
        blob_path = f"data/{k}.csv"
        file_blob_client = container_cl.get_blob_client(blob_path)
        file_urls[k] = file_blob_client.url
        try:
            file_blob_client.upload_blob(buffer.getvalue(), overwrite=True)
        except Exception as e:
            print(f"ERROR: {e}")

    return func.HttpResponse("done", status_code=200)


@app.function_name(name="edi-init")
@app.route(route="init", methods=["POST"])
def init_package(req: func.HttpRequest) -> func.HttpResponse:
    logger.info("RUNNING in the edi function")
    try:
        data = req.get_json()
        package_id = data.get("package_id")
        blob_conn_string = data.get("blob_conn_string")
    except ValueError:
        return func.HttpResponse("invalid input.\n", status_code=400)

    if package_id is None:
        return func.HttpResponse("need a package id to init", status_code=400)

    if blob_conn_string is None:
        return func.HttpResponse(
            "need an azure connection blob string to continue\n", status_code=400
        )

    blob_service_client = BlobServiceClient.from_connection_string(blob_conn_string)
    package_name = f"edi-package-{package_id}"
    container_client = blob_service_client.get_container_client(package_name)
    if container_client.exists():
        return func.HttpResponse(
            "the package you are trying to init already exists!\n", status_code=400
        )

    container_client = blob_service_client.create_container(package_name)
    logger.info(f"created new container for package: {package_name}")
    # create the folders for xml and data
    xml_blob_client = container_client.get_blob_client("xml/")
    xml_blob_client.upload_blob(data="", overwrite=True)
    data_blob_client = container_client.get_blob_client("data/")
    data_blob_client.upload_blob(data="", overwrite=True)

    return func.HttpResponse("package initialization complete!\n")


@app.function_name(name="edi-details")
@app.route(route="details", methods=["GET"])
def package_details(req: func.HttpRequest) -> func.HttpResponse:
    try:
        data = req.get_json()
    except ValueError:
        return func.HttpResponse("invalid input", status_code=400)

    package_id = data.get("package_id")
    blob_conn_string = data.get("blob_conn_string")

    if package_id is None:
        return func.HttpResponse("need package id", status_code=400)

    container_name = f"edi-package-{package_id}"
    blob_service_client = BlobServiceClient.from_connection_string(blob_conn_string)
    container_client = blob_service_client.get_container_client(container_name)
    if not container_client.exists():
        return func.HttpResponse("package id was not found", status_code=400)

    items_in_xml_folder = container_client.list_blobs(name_starts_with="xml/")
    items_in_data_folder = container_client.list_blobs(name_starts_with="data/")
    response_data = {
        "xml": [blob.name for blob in items_in_xml_folder],
        "data": [blob.name for blob in items_in_data_folder],
    }

    return func.HttpResponse(json.dumps(response_data))
