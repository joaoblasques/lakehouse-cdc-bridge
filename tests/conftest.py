import pytest


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    import os

    os.environ["LOCAL_WAREHOUSE"] = str(tmp_path_factory.mktemp("warehouse"))
    from banking_cdc.pipeline.spark import get_spark

    session = get_spark("banking-cdc-tests")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
