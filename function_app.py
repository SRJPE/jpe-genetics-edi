import azure.functions as func
import datetime
import json
import logging
import os
from sqlalchemy import create_engine, Engine, text
import pandas as pd
from bs4 import BeautifulSoup
import requests
from azure.storage.blob import BlobServiceClient

logger = logging.getLogger(__name__)
DB_NAME = os.getenv("DB_NAME") or "runiddb"
DB_URL = os.getenv("DB_URL") or "localhost"
DB_USER = os.getenv("DB_USER") or "emanuel"
DB_PASSWORD = os.getenv("DB_PASSWORD") or "superpassword"
DB_PORT = os.getenv("DB_PORT") or 5432

BASE_URLS = {
    "staging": "https://pasta-s.lternet.edu/",
    "development": "https://pasta-d.lternet.edu/",
    "production": "https://pasta.lternet.edu/",
}

conn_string = (
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_URL}:{DB_PORT}/{DB_NAME}"
)

app = func.FunctionApp()


def get_latest_genetics_results(engine: Engine):
    logging.info("attempting to retrieve latest genetics data")
    q = """
        SELECT DISTINCT ON (gri.sample_id)
            gri.sample_id,
            rt.run_name,
            substring(gri.sample_id FROM '^[^_]+_((?:100|[1-9][0-9]?))_') AS sample_event,
            st.datetime_collected,
            st.fork_length_mm,
            st.field_run_type_id
        FROM
            genetic_run_identification gri
        JOIN
            public.run_type rt
        ON
            rt.id = gri.run_type_id
        JOIN
            public.sample st
        ON st.id = gri.sample_id
        WHERE
            gri.sample_id LIKE '___24%%'
        ORDER BY
            gri.sample_id,
            gri.created_at DESC;
    """
    with engine.connect() as conn:
        data = pd.read_sql_query(q, conn)
    return data


class pastaPackage:
    def __init__(self, container, conn_string, server="staging"):
        self.container = container
        self.conn_string = conn_string
        self._base_url = BASE_URLS.get(server)
        self.blob_service_client = BlobServiceClient.from_connection_string(
            self.conn_string
        )
        self._get_latest_xml_url()
        self._make_soup()

    def _get_package_xmls(self):
        # TODO: get this from the env/az app settings
        container_client = self.blob_service_client.get_container_client(self.container)
        blob_list = list(container_client.list_blobs())

        if len(blob_list) == 0:
            return None

        return sorted(blob_list, key=lambda x: x["last_modified"], reverse=True)

    def _get_latest_xml_url(self):
        latest = self._get_package_xmls()
        if latest:
            blob_url = f"https://{self.blob_service_client.account_name}.blob.core.windows.net/{self.container}/{latest[0].get('name')}"
            self.latest_url = blob_url
        else:
            return None

    def _make_soup(self):
        resp = requests.get(self.latest_url)
        content = resp.content
        soup = BeautifulSoup(content, "lxml-xml")
        self.soup = soup

    def get_package_id(self):
        eml_tag = self.soup.find("eml:eml")
        if eml_tag is not None:
            return eml_tag["packageId"]

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

    def write_xml_to_blob(self):
        current_id = self.get_package_id()
        filename = f"{current_id}.xml"
        blob_client = self.blob_service_client.get_blob_client(
            container=self.container, blob=filename
        )
        xml_content = str(self.soup)
        blob_client.upload_blob(xml_content, overwrite=True)


class simpleEML:
    def __init__(self, url):
        self.url = url
        resp = requests.get(self.url)
        self.soup = BeautifulSoup(resp.content, "lxml-xml")


@app.function_name(name="genetics-EDI-processing")
@app.route(route="fetch-data")
def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("reading the base xml file")
    try:
        xml_base = simpleEML(BASE_XML_URL)
    except Exception as e:
        logging.error(e.__str__())
    logging.info("base xml read in")
    logging.info("This is a test")
    engine = create_engine(conn_string)

    data = get_latest_genetics_results(engine)
    logging.info(data)

    return func.HttpResponse("test is good")
