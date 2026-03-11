import pytest

def assert_df_equality(df1, df2):
    assert sorted(df1.collect()) == sorted(df2.collect())
