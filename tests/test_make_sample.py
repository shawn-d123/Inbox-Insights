from make_sample import reservoir_sample


def test_returns_everything_when_input_is_smaller_than_k():
    rows = [[str(i)] for i in range(5)]
    assert reservoir_sample(rows, k=10) == rows


def test_returns_exactly_k_unique_rows():
    rows = ([str(i)] for i in range(1000))
    sample = reservoir_sample(rows, k=50)

    assert len(sample) == 50
    assert len({row[0] for row in sample}) == 50


def test_same_seed_gives_same_sample():
    first = reservoir_sample(([str(i)] for i in range(1000)), k=20, seed=7)
    second = reservoir_sample(([str(i)] for i in range(1000)), k=20, seed=7)
    assert first == second


def test_spark_fixture_works(spark):
    # Guards against a broken local Java/PySpark setup before the real tests run.
    assert spark.range(3).count() == 3
