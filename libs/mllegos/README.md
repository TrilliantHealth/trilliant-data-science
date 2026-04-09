# `thds.mllegos` Library

Composable components for basic ML tasks

## Why this library exists

At Trilliant Health we operate in one primary domain - medical claims data - so we have a lot of shared
concepts across ML projects. Hence, it would be nice to gather shared utilities related to doing ML on
that data in one place, and not have to reinvent many wheels. This library is meant to be that place.

### Modularity and composability

As the name would suggest, `mllegos` is meant to be home for small, modular, composable components. You
should be able to glue them together into new conglomerations that no one has thought of before - like
legos! If you're adding something, make sure it's small and does one thing well. Ask yourself the
following about any new lego blocks you want to add:

- Does my lego block do one thing well?
- Does it compose well with other lego blocks in the library?
- Does it work on a simple set of inputs of well-known type, not requiring the user to jump through hoops
  to get data into the right shape?
- Does it have esoteric dependencies that would make it hard to use in a different context?

All the legos in the library strive to fulfill these criteria. When the lego blocks are small and do one
thing well, they're easier to test, maintain, understand, and use in new and unexpected settings.

## A sampling of the current legos

### Eval

`sklegos.eval` contains the following legos:

- `cls_report` contains helper functions for working with the dict output of
  `sklearn.metrics.classification_report`
  - `to_pandas` turns it into a pandas DataFrame
  - `multiclass_performance_viz` creates an interactive scatterplot of performance metrics
    (precision/recall/f1-score/support) for all classes
- `viz.basic` contains some slighlty lower-level wrappers for making more custom scatterplots and bar
  charts with `pyecharts`
- see `notebooks/demo_sklearn_classification_report_tools.ipynb` for working examples of all of the above

### Feature extraction

`sklegos.feature_extraction` contains the following legos:

- `OptimizedPrefixEncoding` is useful when dealing with coding systems which use string structure to
  encode hierarchy membership from left to right (e.g. ICD-10 diagnoses/procedures, NUCC taxonomies). It
  dynamically estimates code vocabularies by aggregating evidence from rare codes to shared prefixes with
  better support.

### Feature selection

`sklegos.feature_selection` contains the following legos:

- `DynamicFeatureSelector` is useful when either:

  - you have a large number of features which correlate with your target, but you can't afford to fit a
    model with all of them
  - you have no features or very few features which correlate with your target

  This lego will first try your feature selector of choice, and if that returns too few or too many
  features, will either select some of the remaining features, or further filter the selected features,
  according to a criterion of your choice.

### Imputation

`sklegos.imputation` contains the following legos:

- `ConditionalImputer` is a simple imputation scheme for filling in missing values of a continuous
  feature using point estimates from their conditional distributions given other discrete features.
  Useful when:

  1. your model in incompatible with missing values
  1. the discrete conditioning features are fine-grained enough to provide some specificity about the
     continuous feature
  1. but with good enough support to enable a robust estimate.

  Example: imputing missing patient age in claims using a combination of payer type (medicare, medicaid,
  commercial, etc), sex, and provider specialty.

### Modeling

`sklegos.modeling` contains the following legos:

- `DiscreteFeatureSplit` is a meta-estimator that fits a separate instance of an estimator of a fixed
  architecture on each subset of the training data that corresponds to a unique value of a discrete
  feature. Useful when:

  1. the meaning and utility of many other features vary significantly depending on the value of the
     discrete feature
  1. the discrete feature has enough support in many values to enable robust estimation of the model on
     each subset.

  Example: given a classification task to determine if an ambiguous DRG code is MS or AP, fit a separate
  classifier for each distinct DRG code. Criterion 1 is satisfied often since specific diagnoses or
  patient demographics are highly discriminative for specific DRGs but not globally. E.g. age and sex are
  not highly discriminative globally but are very useful for distinguishing AP 775 (alcohol
  abuse/dependence) from MS 775 (vaginal delivery).

- `TreeStructuredLabelsClassifier` is a meta-estimator that fits a separate instance of a classifier for
  each internal node of an arbitrary tree structure defined on _labels_ (not features). The classifier
  should estimate conditional probabilities of the class given the features (i.e. it should support a
  `predict_proba` method). The meta-classifier estimates class probabilities by applying the chain rule
  of probability to the sub-classifier probabilities along the tree structure. Counterintuitively, this
  can result in more accurate predictions and also a large reduction of computational cost, depending on
  the size of the training set and the cardinality of the label set. A `SubtreeExecutor` callback enables
  pluggable parallelism — subtree fits can be dispatched to remote nodes, threads, or run sequentially
  (the default). A presentation of the idea and some initial research results on toy problems can be
  found
  [here](https://guidebook.trillianthealth.com/data-science/tech-talks/#2025-01-07-classification-with-hierarchical-labels).
  Useful when:

  1. the labels targeted by the classification task have a known hierarchical semantic structure (e.g.
     ICD-10 diagnoses/procedures, NUCC taxonomies)
  1. the cardinality of the labels and the size of the dataset are such that the computational cost of
     training classifier for each label is prohibitive.

  Ideas for future improvement: It would be nice to be able to learn a tree structure when there is no
  standard or explicit one available for the target classes. The point of the meta-classifier is to
  simplify the problem by decomposing it - why not use the confusion matrix from an initial global
  classifier to hierarchically group classes into groups which are highly confusable when tossed in with
  all the others, but might be more distinguishable when considered in isolation?

## Structure of the library

`thds.mllegos` was started with the following initial submodules:

- `feature_extraction` is meant to contain generalized feature extraction utils.
  - code in here should be agnostic to any particular 3rd party library or framework. The idea is that
    you could plug it in to some code that _does_ use such a framework, but you're leaving that interface
    as an extra layer of abstraction.
- `sklegos` is meant to contain any `sklearn`-compatible interfaces for e.g. feature selection,
  ensembling, feature extraction, imputation, etc.
  - Right now it contains `feature_extraction`, `imputation`, and `modeling` submodules, which are meant
    to contain utilities for those tasks
  - Most things in here should inherit from something in `sklearn.base` (e.g. `BaseEstimator`), and be
    compatible with the `sklearn` API
- `util`, as the name suggests, is meant to be a grab bag.
  - The main requirements for adding things there are that
    1. most things in there should be pure python with few if any 3rd party library dependencies
    1. new additions should be in clearly named submodules that encapsulate small modular bits of
       functionality (well, really that goes for the whole library 😄)
  - Right now for instance, there are some type aliases, high-level functional tools, and a couple data
    structures for working with trees and heaps

Note that, while most of the things I have implemented so far are `sklearn`-compatible, that's by no
means a requirement for additions to the library! Hence why I explicitly created an `sklegos` submodule
to put those things in, rather than just including them as root submodules. In fact, I didn't even want
`sklearn` to be a required transitive dependency of the library, hence my addition of an
[`sklearn` extra](https://github.com/TrilliantHealth/ds-monorepo/blob/c1f7225c278ee97db8df2a06d4089f4bbd36e68b/libs/mllegos/pyproject.toml#L43-L44)
in the package spec - you have to explicitly opt in to bring on `sklearn` as a transitive dependency for
your project. I imagine in the future we could have similar submodules for other ML frameworks, e.g. a
`torch-legos` submodule (or `firebricks`? Naming things is fun!).

Feel free to add more submodules as needed (guidelines [here](#contributing))! If there's something you'd
like to see added to the libary, but you don't have time to get to it right away, feel free to add it to
the [wish list](https://github.com/TrilliantHealth/ds-monorepo/issues/2177).

## Contributing

If you have a small piece of code that you think would be useful to others, feel free to add it! Just
follow the guidelines below.

We maintain a standing wish-list of features we'd like to see added to the library in
[this issue](https://github.com/TrilliantHealth/ds-monorepo/issues/2177). If you're adding something,
check to see if it's on that list so you can cross it off!

### Tests

Most utils in the current library have good test coverage at this point. ML utils can be a bit delicate
and nuanced, since they have to handle lots of strange cases that occur out in the wild world of real
data. If you add anything marginally complicated, it should have unit tests that exercise various edge
cases.

### Documentation

Public-facing functions and classes should have docstrings. The main purpose of the component should be
documented, and each of its inputs and outputs should be documented individually. Any quirks of behavior,
gotchas, caveats, and limitations should be documented as well. All ML utils have a valid domain of
applicability, and this domain is usually strictly narrower than the domain of all possible inputs on
which would strictly run without errors. For example, a KNN classifier might struggle with
high-dimensional data, or a linear model might struggle with highly correlated features. Make sure your
users know about these limitations!

### Maintenance and compatibility

We often need to serialize models, and often we use `pickle` to do so. `pickle` is great because it will
serialize almost anything, but it can be a bit of a minefield; it's easy to accidentally make some
production artifact unserializable by changing some existing class def. Tips to keep in mind when making
changes, to avoid breaking serialization of existing models:

- Resist the urge to move class defs around in the codebase
- If you must (sometimes a re-org is a really good thing), and you suspect there are artifacts out there
  using the classes you're moving, keep the original python files around and replace the class defs with
  imports from the new location, with a comment explaining why you're doing this weirdness.
- Resist the urge to significantly change the functionality of existing classes, especially if they're
  used in production artifacts. You could be changing the behavior of existing models inadverdently!
  Usually, if something weird happens, we'll find out through metrics, but it's better to just create a
  new-and-improved version of a class and deprecate the old one, in case you have a substantial feature
  to add.
