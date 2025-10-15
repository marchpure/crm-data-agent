# BigQuery Sample and Test Harness Dataset - Salesforce.com

This repository contains test harness data for Data Analytics samples and solutions built on top of Salesforce.

It has data for the following [Salesforce Objects](https://developer.salesforce.com/docs/atlas.en-us.object_reference.meta/object_reference/sforce_api_objects_concepts.htm):

* Account
* Case
* Case History
* Contact
* Currency Type
* Dated Conversion Rate
* Event
* Lead
* Opportunity
* Opportunity History
* Record Type
* Task
* User

Originally sourced from public test harness dataset `kittycorn-public.sfdc__raw__6_3__us`.

Somewhat cleaned-up and augmented by [@vladkol](https://github.com/vladkol) and [@lsubatin](https://github.com/lsubatin).

## Usage

1. Clone this repository: `git clone https://github.com/vladkol/sfdc-kittycorn`.
2. Change current directory to the repository directory: `cd sfdc-kittycorn`.
3. Upload sample data to your BigQuery project:

```bash
deploy_to_my_project.py --project YOUR_PROJECT_ID --dataset YOUR_DATASET_NAME [--location BIG_QUERY_LOCATION]
# Default `BIG_QUERY_LOCATION` is `US`.
```

> Tables are created according to the source parquet-file names.
> If table already exists, it will be skipped.

## 📄 License

This repository is licensed under the Apache 2.0 License - see the [LICENSE](LICENSE) file for details.

## Disclaimers

This is not an officially supported Google product. This project is not eligible for the [Google Open Source Software Vulnerability Rewards Program](https://bughunters.google.com/open-source-security).

Code and data from this repository are intended for demonstration purposes only. It is not intended for use in a production environment.
