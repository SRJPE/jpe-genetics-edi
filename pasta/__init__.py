import requests


def post_eml(
    xml: str,
    package_id: int,
    username: str,
    password: str,
    base_url: str = "https://pasta.lternet.edu/package/eml/edi",
):
    credentials = (f"uid={username},o=EDI,dc=edirepository,dc=org", password)
    headers = {"Content-Type": "application/xml"}
    response = requests.put(
        f"{base_url}/{package_id}", auth=credentials, headers=headers, data=xml
    )
    return response
