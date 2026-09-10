import pickle

import numpy as np
import pytest
from sklearn.preprocessing import (
    FunctionTransformer,
    MaxAbsScaler,
    MinMaxScaler,
    RobustScaler,
    StandardScaler,
)

from thds.mllegos.sklegos import encoding

# conf classes


def test_confs_are_hashable_and_pickle_stable() -> None:
    confs = (
        encoding.ContinuousFeatureConf(scaling="log1p", fill_value=0.0),
        encoding.OneHotEncoderConf(handle_unknown="infrequent_if_exist", min_frequency=5),
    )
    assert len({confs, pickle.loads(pickle.dumps(confs))}) == 1


def test_is_sparse_properties() -> None:
    assert not encoding.ContinuousFeatureConf().is_sparse
    assert encoding.OneHotEncoderConf().is_sparse
    assert not encoding.OneHotEncoderConf(sparse_output=False).is_sparse


# scaling_transformer


@pytest.mark.parametrize(
    "scaling, transformer_cls",
    [
        pytest.param("log1p", FunctionTransformer, id="log1p"),
        pytest.param("standard", StandardScaler, id="standard"),
        pytest.param("robust", RobustScaler, id="robust"),
        pytest.param("min_max", MinMaxScaler, id="min-max"),
        pytest.param("max_abs", MaxAbsScaler, id="max-abs"),
    ],
)
def test_scaling_transformer_mapping(scaling, transformer_cls) -> None:
    assert isinstance(encoding.scaling_transformer(scaling), transformer_cls)


def test_log1p_transformer_casts_ints_and_names_features() -> None:
    transformer = encoding.scaling_transformer("log1p")
    result = transformer.fit_transform(np.array([[0], [1]], dtype=int))
    np.testing.assert_allclose(result, [[0.0], [np.log(2)]])
    assert list(transformer.get_feature_names_out(["x"])) == ["x"]


# one_hot_encoder


def test_one_hot_encoder_wires_conf_through() -> None:
    conf = encoding.OneHotEncoderConf(
        handle_unknown="infrequent_if_exist", min_frequency=3, sparse_output=False
    )
    enc = encoding.one_hot_encoder(conf, dtype=np.float32)
    assert enc.handle_unknown == "infrequent_if_exist"
    assert enc.min_frequency == 3
    assert enc.sparse_output is False
    assert enc.dtype == np.float32


def test_one_hot_encoder_default_dtype_is_sklearn_default() -> None:
    assert encoding.one_hot_encoder(encoding.OneHotEncoderConf()).dtype == np.float64
