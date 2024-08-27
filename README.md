# EDI Publish AZ Function

## Usage

You need to have an id from from edi in order to use this function for publishing.

1. First run this function to setup the folders on the container.

```bash
curl https://edi-workflows.azurewebsites.net/api/publish?package_number=edi-<your-package-id>&code=<container-access-code>
```

this creates a blob for `edi-<package-id>` within that folder two more are created a `data` and an `xml` one.
