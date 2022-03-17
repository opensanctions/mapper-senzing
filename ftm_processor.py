import json
import click
import logging
from pprint import pprint
from typing import Dict, List

from followthemoney import model
from followthemoney.proxy import EntityProxy
from followthemoney.types import registry

log = logging.getLogger("ftm_processor")
ADDRESSES: Dict[str, EntityProxy] = {}
IDENTS: Dict[str, List[EntityProxy]] = {}


def read_entities(source_file):
    log.info("Caching aux entities: %r", source_file)
    with open(source_file, "r") as fh:
        while line := fh.readline():
            data = json.loads(line)
            entity = model.get_proxy(data)
            if entity.schema.is_a("Address"):
                ADDRESSES[entity.id] = entity
            if entity.schema.is_a("Identification"):
                for holder in entity.get("holder"):
                    if holder not in IDENTS:
                        IDENTS[holder] = []
                    IDENTS[holder].append(entity)

    log.info("Reading entities: %r", source_file)
    with open(source_file, "r") as fh:
        while line := fh.readline():
            data = json.loads(line)
            entity = model.get_proxy(data)
            if entity.schema.name in (
                "Person",
                "Organization",
                "Company",
                "LegalEntity",
                "PublicBody",
            ):
                yield entity


def map_feature(entity, features, prop, attr):
    for value in entity.get(prop, quiet=True):
        features.append({attr: value})


def transform(entity: EntityProxy):
    record = {
        "DATA_SOURCE": "OPENSANCTIONS",
        "RECORD_ID": entity.id,
    }
    if entity.schema.name == "Person":
        record["REC_TYPE"] = "PERSON"
    if entity.schema.name in ("Organization", "Company", "PublicBody"):
        record["REC_TYPE"] = "ORGANIZATION"

    is_org = entity.schema.is_a("Organization")

    features = []
    for name in entity.get_type_values(registry.name):
        name_type = "PRIMARY" if name == entity.caption else "ALIAS"
        name_field = "NAME_ORG" if is_org else "NAME_FULL"
        name = {"NAME_TYPE": name_type, name_field: name}
        features.append(name)

    for addr_id in entity.get("addressEntity"):
        addr = ADDRESSES.get(addr_id)
        addr_data = {
            "ADDR_FULL": addr.first("full"),
            "ADDR_LINE1": addr.first("street"),
            "ADDR_LINE2": addr.first("street2"),
            "ADDR_CITY": addr.first("city"),
            "ADDR_STATE": addr.first("state"),
            "ADDR_COUNTRY": addr.first("country"),
            "ADDR_POSTAL_CODE": addr.first("postalCode"),
        }
        features.append(addr_data)

    for gender in entity.get("gender", quiet=True):
        if gender == "male":
            features.append({"GENDER": "M"})
        if gender == "female":
            features.append({"GENDER": "F"})

    map_feature(entity, features, "birthDate", "DATE_OF_BIRTH")
    map_feature(entity, features, "deathDate", "DATE_OF_DEATH")
    map_feature(entity, features, "birthPlace", "PLACE_OF_BIRTH")
    map_feature(entity, features, "nationality", "NATIONALITY")
    map_feature(entity, features, "country", "CITIZENSHIP")
    map_feature(entity, features, "incorporationDate", "REGISTRATION_DATE")
    map_feature(entity, features, "jurisdiction", "REGISTRATION_COUNTRY")
    map_feature(entity, features, "website", "WEBSITE_ADDRESS")
    map_feature(entity, features, "email", "EMAIL_ADDRESS")
    map_feature(entity, features, "phone", "PHONE_NUMBER")
    map_feature(entity, features, "passportNumber", "PASSPORT_NUMBER")
    map_feature(entity, features, "idNumber", "NATIONAL_ID_NUMBER")
    map_feature(entity, features, "taxNumber", "TAX_ID_NUMBER")

    for adj in IDENTS.get(entity.id, []):
        if adj.schema.is_a("Passport"):
            adj_data = {
                "PASSPORT_NUMBER": adj.first("number"),
                "PASSPORT_COUNTRY": adj.first("country"),
            }
        else:
            adj_data = {
                "NATIONAL_ID_NUMBER": adj.first("number"),
                "NATIONAL_ID_COUNTRY": adj.first("country"),
            }
        adj_data = {k: v for k, v in adj_data.items() if v is not None}
        if len(adj_data):
            features.append(adj_data)

    record["FEATURES"] = features
    return record


@click.command()
@click.argument("source_file", type=click.Path(exists=True, file_okay=True))
@click.argument("target_file", type=click.Path())
def process_senzing(source_file, target_file):
    logging.basicConfig(level=logging.INFO)

    with open(target_file, "w") as fh:
        for entity in read_entities(source_file):
            record = transform(entity)
            fh.write(json.dumps(record))
            fh.write("\n")


if __name__ == "__main__":
    process_senzing()
