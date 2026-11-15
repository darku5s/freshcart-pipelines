if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
import os

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_exporter
def export_metrics(metrics, *args, **kwargs):
    metrics.to_csv("iris_metrics.csv", index=False)
    print("Saved metrics to iris_metrics.csv")


@test
def test_output(*args):
    assert os.path.exists("iris_metrics.csv")