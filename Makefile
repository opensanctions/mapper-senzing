
data-folder:
	mkdir -p data/exports

data/default.json: data-folder
	wget -q -c -O data/default.json https://data.opensanctions.org/datasets/latest/default/entities.ftm.json

data/sanctions.json: data-folder
	wget -q -c -O data/sanctions.json https://data.opensanctions.org/datasets/latest/sanctions/entities.ftm.json

process-default: data/default.json
	python ftm_processor.py data/default.json data/exports/default.json

process-sanctions: data/sanctions.json
	python ftm_processor.py data/sanctions.json data/exports/sanctions.json

process: process-default process-sanctions