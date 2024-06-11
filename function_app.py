from io import StringIO
import azure.functions as func
import datetime
import json
import logging
import os
from sqlalchemy import create_engine, Engine, text
import pandas as pd
from bs4 import BeautifulSoup
import requests
from azure.storage.blob import BlobServiceClient, PublicAccess

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


class EDIPipeline:
    """
    EDI Pipeline

    - xml_container: the blob container where metadata xml files are stored
    - server: the edi server to use for data uploads
    """

    def __init__(
        self,
        az_blob_container: str,
        server="staging",
    ):
        self.az_blob_container = az_blob_container
        self._base_url = BASE_URLS.get(server)

        # self._get_latest_xml_url()
        # self._make_soup()

    def az_blob_connect(self, conn_str: str):
        self.blob_service_client = BlobServiceClient.from_connection_string(conn_str)
        container_client = self.blob_service_client.get_container_client(
            self.az_blob_container
        )
        if not container_client.exists():
            logging.info("container not found, creating from template...")
            container_client.create_container(public_access=PublicAccess.CONTAINER)
            init_files = ["xml/init.txt", "data/init.txt"]
            for file in init_files:
                container_client.upload_blob(file, b"")
            logging.info("creation succesfull")

        self.container_client = container_client

    def db_connect(self, dbname, user, password, host, port):
        db_conn_string = (
            f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
        )
        self.engine = create_engine(db_conn_string)

    def _get_package_xmls(self):
        # TODO: get this from the env/az app settings
        blob_list = list(self.container_client.list_blobs(name_starts_with="xml/"))

        if len(blob_list) == 0:
            return None

        return sorted(blob_list, key=lambda x: x["last_modified"], reverse=True)

    def _get_latest_xml_url(self):
        latest = self._get_package_xmls()
        if latest:
            blob_url = f"https://{self.blob_service_client.account_name}.blob.core.windows.net/{self.az_blob_container}/{latest[0].get('name')}"
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
        filename = f"xml/{current_id}.xml"
        blob_client = self.blob_service_client.get_blob_client(
            container=self.az_blob_container, blob=filename
        )
        xml_content = str(self.soup)
        blob_client.upload_blob(xml_content, overwrite=True)

    def write_csv_to_blob(self, filename: str, data: pd.DataFrame):
        blob_client = self.blob_service_client.get_blob_client(
            container=self.az_blob_container, blob=f"data/{filename}.csv"
        )
        csv_binary = StringIO()
        data.to_csv(csv_binary, index=False)
        csv_content = csv_binary.getvalue()

        blob_client.upload_blob(csv_content)

    def get_latest_genetics_data(self):
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
        with self.engine.connect() as conn:
            data = pd.read_sql_query(q, conn)
        return data

    def latest_data_db_to_blob(self):
        """
        perform all operations to get the latest data from database and upload to blob
        """
        latest_data = self.get_latest_genetics_data()
        new_package_id = self.package_id_revision_increment()
        filename = f"genetics-resuts-{new_package_id}.csv"
        self.write_csv_to_blob(filename)

    def latest_xml_to_blob(self):
        self.update_package_id_tag()
        self.write_xml_to_blob()


@app.function_name(name="genetics-EDI-processing")
@app.route(route="fetch-data")
def main(req: func.HttpRequest) -> func.HttpResponse:
    conn_string = os.getenv("AZURE_BLOB_CONN_STRING") or ""
    p = EDIPipeline("edi-1617")
    p.az_blob_connect(conn_string)
    p.db_connect(DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT)
    data = p.get_latest_genetics_data()
    p.write_csv_to_blob("genetics-data", data)
    # # p.latest_data_db_to_blob()
    # p.latest_xml_to_blob()

    return func.HttpResponse("EDI Pipeline Excecution Complete")
