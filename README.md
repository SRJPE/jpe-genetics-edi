# EDI Publish AZ Function

## Usage

You need to have an id from from edi in order to use this function for publishing.

1. First run this function to setup the folders on the container.

```bash
curl https://edi-workflows.azurewebsites.net/api/publish?package_number=edi-<your-package-id>&code=<container-access-code>
```

this creates a blob for `edi-<package-id>` within that folder two more are created a `data` and an `xml` one.

2. In order to bootstrap the process you must drop in a completed first version of the xml metadata file. You 
have two options to create this, using the `emlaide` R package or the [ezEML Webiste](https://ezeml.edirepository.org/eml/auth/login).

3. Create an sql query to will be used to pull data from the database, a connection string to the databaes is also
needed.
