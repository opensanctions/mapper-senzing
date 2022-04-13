import json
import click
import logging
from pathlib import Path
from pprint import pprint
from followthemoney.dedupe import Judgement
from nomenklatura.resolver import Resolver, Identifier
from nomenklatura.util import PathLike

log = logging.getLogger("senzing_resolver")


def read_senzing_export(senzing_export: PathLike):
    with open(senzing_export, "r") as fh:
        while True:
            line = fh.readline()
            if not line:
                break
            yield json.loads(line)


@click.command()
@click.argument("senzing_export", type=click.Path(exists=True, file_okay=True))
@click.argument("resolver_file", type=click.Path())
def make_resolver(senzing_export: PathLike, resolver_file: PathLike):
    logging.basicConfig(level=logging.INFO)
    resolver = Resolver.load(Path(resolver_file).resolve())

    for idx, entity in enumerate(read_senzing_export(senzing_export)):
        if idx % 10000 == 0 and idx > 0:
            log.info("Converting record groups: %d ...", idx)
        resolved = entity.pop("RESOLVED_ENTITY", {})
        # related = entity.pop("RELATED_ENTITIES", [])
        records = resolved.pop("RECORDS", [])
        if len(records) == 1:
            continue

        if len(records) > 1000:
            log.warning("Mega-node: %d entities (skipping)", len(records))
            continue

        target = None
        for record in records:
            record_id = Identifier.get(record["RECORD_ID"])
            if target is None:
                target = record_id
                continue

            if not resolver.check_candidate(target, record_id):
                # log.warning("Logic error: %s <> %s", target, record_id)
                continue

            # log.warning("Match: %s == %s", target, record_id)
            target = resolver.decide(
                target,
                record_id,
                judgement=Judgement.POSITIVE,
                user="senzing",
            )
            # pprint((resolved["ENTITY_NAME"], record))

    print("done, saving")
    resolver.save()


if __name__ == "__main__":
    make_resolver()
