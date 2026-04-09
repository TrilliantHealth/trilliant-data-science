import pandas as pd

from thds.mllegos.sklegos.imputation.conditional_imputer import ConditionalImputer


def test_conditional_imputer():
    X = pd.DataFrame(
        dict(
            a=[1, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, None],
            b=[None, 2, 3, 4, 5, 7, 7, 7, None, 8, None, None],
        )
    ).astype(float)
    imputer = ConditionalImputer(["a"], {"b": "median"}, min_freq=3)
    X_imputed = imputer.fit_transform(X)

    pd.testing.assert_frame_equal(
        X_imputed,
        pd.DataFrame(dict(a=X["a"], b=[3.5, 2, 3, 4, 5, 7, 7, 7, 7, 8, 6, 6])).astype(float),
    )
