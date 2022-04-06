# Senzing JSON generator

This is a simple mapper to convert the [OpenSanctions Consolidated Sanctioned Entities](https://www.opensanctions.org/datasets/sanctions/) to JSON that can be loaded into Senzing.

The data is automatically converted every day by the GitHub Actions associated with this repository. You can fetch the Senzing form of the data from these URLs:

* https://data.opensanctions.org/contrib/senzing/exports/sanctions.json (Sanctions data)
* https://data.opensanctions.org/contrib/senzing/exports/default.json (Full OpenSanctions with PEPs)

Download the files and then add the OPENSANCTIONS data source to the Senzing configuration and load the JSON file in.

**IMPORTANT:** Please be advised that OpenSanctions data is subject to non-commercial licensing conditions (CC-BY-NC). If you intend to use it in a commercial setting, please refer to the [data licensing](https://opensanctions.org/licensing/) page.

### Running the processor locally

Install the dependencies in `requirements.txt` and then check out the `Makefile` to see how to run the processor locally. 

### Senzing format documentation

* https://senzing.zendesk.com/hc/en-us/articles/231925448-Generic-Entity-Specification-JSON-CSV-Mapping
* https://senzing.zendesk.com/hc/en-us/article_attachments/4405461248915/Senzing_Generic_Entity_Specification_v2.8.1.pdf 
