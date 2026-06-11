from app.services.connectors.catalog import (
    CATALOG_BY_SLUG,
    ConnectorCategory,
    Stability,
    catalog,
)


def test_catalog_covers_enterprise_sources() -> None:
    expected = {
        "sqlite",
        "notion",
        "google_docs",
        "quip",
        "github",
        "confluence",
        "postgres",
        "snowflake",
        "redshift",
        "sql_server",
        "databricks",
        "bigquery",
        "mysql",
        "trino",
        "airflow",
        "dbt",
        "fivetran",
        "dagster",
        "prefect",
        "airbyte",
        "openai",
    }
    assert expected == set(CATALOG_BY_SLUG)


def test_every_connector_has_schema_logo_and_sync_behavior() -> None:
    for connector in catalog():
        assert connector.logo_key
        assert connector.credential_schema
        assert connector.sync_behavior
        assert connector.production_notes


def test_connector_categories_are_present() -> None:
    categories = {connector.category for connector in catalog()}
    assert categories == {
        ConnectorCategory.KNOWLEDGE,
        ConnectorCategory.DATA_STORE,
        ConnectorCategory.ORCHESTRATION,
        ConnectorCategory.LLM,
    }


def test_public_catalog_payload_has_no_verification_label() -> None:
    payload = [
        {
            **item.model_dump(exclude={"local_verification"}),
            "credential_schema": [field.model_dump() for field in item.credential_schema],
        }
        for item in catalog()
    ]

    assert len(payload) == 21
    for item in payload:
        assert "local_verification" not in item
        assert "verification" not in item


def test_airflow_is_stable_for_demo_catalog() -> None:
    airflow = CATALOG_BY_SLUG["airflow"]
    assert airflow.stability == Stability.STABLE
    assert airflow.known_issues == []
