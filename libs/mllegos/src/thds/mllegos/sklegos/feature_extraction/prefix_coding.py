import typing as ty

import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.utils.validation import check_is_fitted

from thds.core.log import getLogger

from ...feature_extraction import prefix_coding, tokens
from ...util import aliases, functional
from ..types import Any1DArray

K = ty.TypeVar("K", bound=ty.Hashable)

_LOGGER = getLogger(__name__)


class _CollectionAsMapping(ty.Mapping[K, K]):
    """A set-like object that also behaves like a mapping from keys to themselves."""

    def __init__(self, collection: ty.Collection[K]):
        self.collection = collection

    def __len__(self) -> int:
        return len(self.collection)

    def __contains__(self, item) -> bool:
        return item in self.collection

    def __getitem__(self, item: K) -> K:
        if item in self.collection:
            return item
        raise KeyError(item)

    def get(self, key: K, default=None):
        if key in self.collection:
            return key
        return default

    def __iter__(self) -> ty.Iterator[K]:
        return iter(self.collection)

    def keys(self):
        return self.collection

    def values(self):
        return self.collection


class OptimizedPrefixEncoding(BaseEstimator, TransformerMixin):
    """Encode a string-valued feature using an optimized prefix encoding scheme. The values
    should be "code-like", with prefixes carrying hierarchical information about the meaning of each
    value. For example, ICD-10 diagnosis codes satisfy this requirement.
    """

    def __init__(
        self,
        min_count: int,
        unknown_value: ty.Optional[str] = None,
        null_value: ty.Optional[str] = None,
        normalizer: ty.Optional[aliases.Normalizer[str]] = None,
        array_valued: bool = False,
    ):
        """Create a new instance of the OptimizedPrefixEncoding transformer.

        :param min_count: The min support allowed for any code or prefix after pruning
        :param array_valued: Whether the input data is array-valued (i.e. a Series of lists of strings)
        :param unknown_value: The value to use for unknown codes or those which can't be prefix-encoded
        :param null_value: The value to use for null values
        """
        self.min_count = min_count
        self.array_valued = array_valued
        self.normalizer = normalizer
        self.unknown_value = unknown_value
        self.null_value = null_value

    def fit(self, X: Any1DArray, y=None):
        """Fit the prefix encoding to the data.

        :param X: A pandas Series of string values to encode
        :param y: Ignored
        :return: self
        """
        X_ = pd.Series(X, copy=False)
        if self.array_valued:
            counts = X_.explode().value_counts(dropna=True)
        else:
            counts = X_.value_counts(dropna=True)

        if self.normalizer is not None:
            counts = counts.groupby(counts.index.map(self.normalizer), dropna=True).sum()

        self.prefix_counts_ = prefix_coding.optimize_prefix_coding(
            counts,
            min_count=self.min_count,
        )
        self._set_encoder()
        return self

    def get_feature_names_out(self, input_features=None):
        # Series -> Series; no change in feature names
        # providing this implementation allows this to work in the context of a pipeline when it's upstream of some
        # other transformer that produces a 2D array
        return None

    def _set_encoder(self):
        find_prefix = prefix_coding.prefix_normalizer(self.prefix_counts_)
        self.encoder_ = tokens.TokenLookup(
            _CollectionAsMapping(self.prefix_counts_),
            # map tokens to themselves rather than their counts, while avoiding a copy
            self.unknown_value,
            self.null_value,
            functional.allow_na(
                (
                    find_prefix
                    if self.normalizer is None
                    else functional.pipe(self.normalizer, functional.allow_na(find_prefix))
                ),
            ),
        )

    def transform(self, X: Any1DArray) -> pd.Series:
        """Transform the input data using the learned prefix encoding.

        :param X: A pandas Series of string values to encode
        :return: A pandas Series of the same length as X, with the encoded values
        """
        check_is_fitted(self)
        X_ = pd.Series(X, copy=False)
        if self.array_valued:
            return X_.map(functional.list_map_filter_null(self.encoder_), na_action="ignore")
        else:
            return X_.apply(self.encoder_)
