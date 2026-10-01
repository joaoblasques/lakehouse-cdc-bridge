from banking_cdc.pipeline.config import kafka_conf, load_config, topic_map


def test_every_configured_table_has_a_topic():
    cfg = load_config()
    assert topic_map(cfg["sources"]["sqlserver_cards"]) == {
        "dbo.customers": "banking.cdc.cards.customers",
        "dbo.accounts": "banking.cdc.cards.accounts",
        "dbo.card_transactions": "banking.cdc.cards.card_transactions",
    }
    assert topic_map(cfg["sources"]["partner_fileshare"]) == {
        "partner_transfers": "banking.cdc.partner.transfers"
    }


def test_kafka_conf_switches_to_confluent_cloud_when_api_key_present():
    local = {"kafka_bootstrap": "localhost:9092"}
    assert kafka_conf(lambda k, f: local[f"{k}_{f}"]) == {"bootstrap.servers": "localhost:9092"}
    cloud = local | {"kafka_api_key": "k", "kafka_api_secret": "s"}
    conf = kafka_conf(lambda k, f: cloud[f"{k}_{f}"])
    assert conf["security.protocol"] == "SASL_SSL"
    assert conf["sasl.username"] == "k"


def test_bundle_for_each_covers_every_configured_source():
    import json
    from pathlib import Path

    import yaml

    bundle = yaml.safe_load((Path(__file__).parents[1] / "databricks.yml").read_text())
    capture = bundle["resources"]["jobs"]["banking_cdc"]["tasks"][0]
    assert json.loads(capture["for_each_task"]["inputs"]) == list(load_config()["sources"])
