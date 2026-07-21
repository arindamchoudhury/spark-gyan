# MLlib / ML

> Source: `release-notes/spark_all_changelogs.txt`. Timeline rows below are generated from `_catalog.jsonl`; prose outside the AUTO markers is hand-written.

## How it evolved

### 0.x era — origins

MLlib was introduced in 0.8.0 as a standard library of machine learning and optimization algorithms, launching with seven: SVMs, logistic regression, several regularized linear regression variants, KMeans clustering, and alternating least squares (ALS) collaborative filtering. 0.8.1 added an ALS variant for implicit feedback matrix factorization.

0.9.0 made MLlib available in Python (operating on NumPy data) and added a Naive Bayes classifier, plus support for predicting ratings for multiple items in parallel with ALS. The 0.9.1 patch release focused entirely on ALS refinements — optimized memory usage and YtY computation, support for negative implicit input, an ability to set a random seed, and faster feature construction with intercept.

### 1.x era — ML Pipelines and the algorithm explosion

MLlib's algorithm surface expanded fast: 1.0.0 added decision trees, distributed SVD/PCA, and L-BFGS; 1.1.0 added a statistics package (stratified sampling, correlations, chi-squared tests), Word2Vec/TF-IDF, and nonnegative matrix factorization. The pivotal change was 1.2.0, previewing the new `spark.ml` package — a learning-pipelines API chaining algorithms with varying parameters, built on `SchemaRDD` for Spark SQL interop — alongside random forests and gradient-boosted trees. 1.3.0 ported the pipeline API onto the new DataFrame abstraction and added LDA topic modeling, multinomial logistic regression, GMM clustering, and FP-growth. 1.4.0 stabilized the DecisionTree/ensemble APIs (SPARK-6113) and added elastic-net logistic regression (SPARK-7262) and PMML export (SPARK-1406); 1.6.0 closed the era with univariate/bivariate DataFrame statistics (SPARK-10385).

### 2.x era — the DataFrame-based API catches up to spark.mllib

2.0.0 pushed algorithm parity across languages: SparkR gained MLlib APIs for GLMs, naive Bayes, k-means, and survival regression, PySpark added LDA, Gaussian Mixture Models, and generalized linear regression, and the DataFrame-based API gained Bisecting K-Means and `MaxAbsScaler`. 2.1.0 made model persistence backward-compatible with Spark 1.x saved models (SPARK-16000). 2.2.0 added FPGrowth frequent-pattern mining (SPARK-14503), DataFrame-native ChiSquare tests and correlation (SPARK-19635, SPARK-19636), and Gradient Boosted Trees in Python/R (SPARK-18239). 2.3.0 was the busiest release, adding multi-column support to `Bucketizer`/`QuantileDiscretizer`, `ClusteringEvaluator` with silhouette metrics (SPARK-14516), and Huber-loss robust linear regression (SPARK-3181) — collectively closing the gap between the older `spark.mllib` RDD API and the newer DataFrame-based `spark.ml`.

### 3.x era — blockified linear models and ML on Connect

3.0.0 rounded out the DataFrame-based ML API with Gaussian Naive Bayes (SPARK-16872), sample weights for decision trees and random forests, and closer Scala/Python parity (SPARK-28958). 3.1.1's headline work was blockifying input vectors for the linear models — LinearSVC, LogisticRegression, LinearRegression, and AFT (SPARK-30642, SPARK-30659, SPARK-30660, SPARK-31656) — batching instances into blocks for faster BLAS-level computation, alongside a vectorized BLAS implementation carried into 3.2.0 (SPARK-33882). 3.2.0 also added `UnivariateFeatureSelector` to unify the older selector classes. 3.3.0 and 3.4.0 were comparatively quiet, mostly incremental optimizer and ALS-shuffle work. 3.5.0 introduced the first ML-on-Spark-Connect surface (SPARK-43516): estimator/transformer/model/evaluator interfaces, pipeline and cross-validator support, and PyTorch Distributor compatibility with Connect (SPARK-42993).

### 4.x era — ML Connect polish, quiet maintenance

MLlib's 4.x footprint is small, reflecting a codebase in steady maintenance rather than active growth. 4.0.0 made several transformers support nested input columns (SPARK-48463), avoided redundant NNZ recomputation in `Binarizer` (SPARK-45757), added built-in Vector validation (SPARK-45547), and added Target Encoding to `ml.feature` (SPARK-37178) — the last new estimator of the era.

4.1.0 improved ML on Spark Connect (SPARK-51236), continuing the Connect-parity work started in 3.5.0, and 4.1.1 made the pipelines internal package private (SPARK-54689). No 4.x release introduces a new algorithm family; the pattern from 3.1.1 onward — blockified linear models, then Connect support — has settled into incremental polish on an already-complete DataFrame-based API.

## Timeline

<!-- AUTO:timeline START -->
| Release | JIRA | Type | Title |
|---|---|---|---|
| 0.8.0 | — | prose | MLlib machine learning library introduced |
| 0.8.0 | — | prose | MLlib ships seven algorithms (SVM, logistic regression, linear regression variants, KMeans, ALS) |
| 0.8.1 | — | prose | New ALS variant for implicit feedback matrix factorization |
| 0.9.0 | — | prose | MLlib available in Python (NumPy-based) |
| 0.9.0 | — | prose | New Naive Bayes classification algorithm |
| 0.9.0 | — | prose | ALS models predict ratings for multiple items in parallel |
| 0.9.1 | — | prose | Optimized ALS memory usage |
| 0.9.1 | — | prose | Support for negative implicit input in ALS |
| 0.9.1 | [SPARK-1237](https://issues.apache.org/jira/browse/SPARK-1237) | prose | Optimized YtY computation for implicit ALS |
| 0.9.1 | [SPARK-1238](https://issues.apache.org/jira/browse/SPARK-1238) | prose | Ability to set a random seed in ALS |
| 0.9.1 | [SPARK-1260](https://issues.apache.org/jira/browse/SPARK-1260) | prose | Faster construction of features with intercept |
| 1.0.0 | — | prose | MLlib adds sparse feature vector support in Scala/Java/Python |
| 1.0.0 | — | prose | MLlib adds decision trees, distributed SVD/PCA, model evaluation, L-BFGS |
| 1.1.0 | — | prose | New MLlib statistics package (stratified sampling, correlations, chi-squared, random data) |
| 1.1.0 | — | prose | Feature extraction utilities: Word2Vec and TF-IDF |
| 1.1.0 | — | prose | Feature transformation utilities: normalization and standard scaling |
| 1.1.0 | — | prose | Nonnegative matrix factorization support |
| 1.1.0 | — | prose | SVD via Lanczos support |
| 1.1.0 | — | prose | Decision tree algorithm added in Python and Java |
| 1.1.0 | — | prose | Tree aggregation primitive added to optimize existing algorithms |
| 1.1.0 | — | prose | MLlib 1.1 performance improves 2-3X, up to 5X for large decision trees |
| 1.2.0 | — | prose | New spark.ml package previews learning pipelines API |
| 1.2.0 | — | prose | New ML package uses SchemaRDD for ML datasets (Spark SQL interop) |
| 1.2.0 | — | prose | Decision trees extended with random forests and gradient-boosted trees |
| 1.3.0 | — | prose | LDA (topic modeling) added to MLlib |
| 1.3.0 | — | prose | Multinomial logistic regression for multiclass classification |
| 1.3.0 | — | prose | Gaussian mixture model (GMM) for clustering |
| 1.3.0 | — | prose | Power iteration clustering |
| 1.3.0 | — | prose | FP-growth for frequent pattern mining |
| 1.3.0 | — | prose | Block matrix abstraction for distributed linear algebra |
| 1.3.0 | — | prose | Initial support for MLlib model import/export in exchangeable format |
| 1.3.0 | — | prose | k-means and ALS implementation updates yield significant performance gains |
| 1.3.0 | — | prose | ML pipeline API ported to support the DataFrame abstraction |
| 1.4.0 | [SPARK-1406](https://issues.apache.org/jira/browse/SPARK-1406) | prose | PMML model evaluation support via MLlib |
| 1.4.0 | [SPARK-3066](https://issues.apache.org/jira/browse/SPARK-3066) | prose | Support recommendAll in matrix factorization model |
| 1.4.0 | [SPARK-4588](https://issues.apache.org/jira/browse/SPARK-4588) | prose | Add API for feature attributes |
| 1.4.0 | [SPARK-4894](https://issues.apache.org/jira/browse/SPARK-4894) | prose | Bernoulli naive Bayes |
| 1.4.0 | [SPARK-5563](https://issues.apache.org/jira/browse/SPARK-5563) | prose | LDA with online variational inference |
| 1.4.0 | [SPARK-5884](https://issues.apache.org/jira/browse/SPARK-5884) | prose | A variety of feature transformers for ML pipelines |
| 1.4.0 | [SPARK-5995](https://issues.apache.org/jira/browse/SPARK-5995) | prose | Make ML Prediction Developer APIs public |
| 1.4.0 | [SPARK-6113](https://issues.apache.org/jira/browse/SPARK-6113) | prose | Stabilize DecisionTree and ensembles APIs |
| 1.4.0 | [SPARK-7015](https://issues.apache.org/jira/browse/SPARK-7015) | prose | OneVsRest multiclass to binary reduction |
| 1.4.0 | [SPARK-7262](https://issues.apache.org/jira/browse/SPARK-7262) | prose | Binary LogisticRegression with L1/L2 (elastic net) |
| 1.4.0 | [SPARK-7381](https://issues.apache.org/jira/browse/SPARK-7381) | prose | Python API for ML pipelines |
| 1.5.0 | — | prose | LDA improvements: online performance, asymmetric doc concentration, perplexity, etc. |
| 1.5.0 | — | prose | Trees and ensembles improvements: class probabilities, feature importance, thresholds, checkpointing |
| 1.5.0 | — | prose | GMM distributes matrix inversions |
| 1.5.0 | — | prose | Model summary for linear and logistic regression |
| 1.5.0 | — | prose | Python API additions: distributed matrices, streaming k-means/linear models, LDA, power iteration clustering |
| 1.5.0 | — | prose | Tuning and evaluation: train-validation split and multiclass classification evaluator |
| 1.5.0 | [SPARK-1856](https://issues.apache.org/jira/browse/SPARK-1856) | Umbrella | Standardize MLlib interfaces |
| 1.5.0 | [SPARK-3258](https://issues.apache.org/jira/browse/SPARK-3258) | Umbrella | Python API for streaming MLlib algorithms |
| 1.5.0 | [SPARK-4362](https://issues.apache.org/jira/browse/SPARK-4362) | Improvement | Make prediction probability available in NaiveBayesModel |
| 1.5.0 | [SPARK-4752](https://issues.apache.org/jira/browse/SPARK-4752) | New Feature | Classifier based on artificial neural network |
| 1.5.0 | [SPARK-5133](https://issues.apache.org/jira/browse/SPARK-5133) | New Feature | Feature Importance for Random Forests |
| 1.5.0 | [SPARK-5567](https://issues.apache.org/jira/browse/SPARK-5567) | Improvement | Add prediction methods to LDA |
| 1.5.0 | [SPARK-5962](https://issues.apache.org/jira/browse/SPARK-5962) | New Feature | [MLLIB] Python support for Power Iteration Clustering |
| 1.5.0 | [SPARK-6001](https://issues.apache.org/jira/browse/SPARK-6001) | Improvement | K-Means clusterer should return the assignments of input points to clusters |
| 1.5.0 | [SPARK-6129](https://issues.apache.org/jira/browse/SPARK-6129) | New Feature | Create MLlib metrics user guide with algorithm definitions and complete code examples. |
| 1.5.0 | [SPARK-6164](https://issues.apache.org/jira/browse/SPARK-6164) | Improvement | CrossValidatorModel should keep stats from fitting |
| 1.5.0 | [SPARK-6192](https://issues.apache.org/jira/browse/SPARK-6192) | Umbrella | Enhance MLlib's Python API (GSoC 2015) |
| 1.5.0 | [SPARK-6259](https://issues.apache.org/jira/browse/SPARK-6259) | Improvement | Python API for LDA |
| 1.5.0 | [SPARK-6390](https://issues.apache.org/jira/browse/SPARK-6390) | New Feature | Add MatrixUDT in PySpark |
| 1.5.0 | [SPARK-6487](https://issues.apache.org/jira/browse/SPARK-6487) | New Feature | Add sequential pattern mining algorithm PrefixSpan to Spark MLlib |
| 1.5.0 | [SPARK-6683](https://issues.apache.org/jira/browse/SPARK-6683) | Improvement | Handling feature scaling properly for GLMs |
| 1.5.0 | [SPARK-6793](https://issues.apache.org/jira/browse/SPARK-6793) | Improvement | Implement perplexity for LDA |
| 1.5.0 | [SPARK-7045](https://issues.apache.org/jira/browse/SPARK-7045) | Improvement | Word2Vec: avoid intermediate representation when creating model |
| 1.5.0 | [SPARK-7127](https://issues.apache.org/jira/browse/SPARK-7127) | Improvement | Broadcast spark.ml tree ensemble models for predict |
| 1.5.0 | [SPARK-7131](https://issues.apache.org/jira/browse/SPARK-7131) | Improvement | Move tree,forest implementation from spark.mllib to spark.ml |
| 1.5.0 | [SPARK-7368](https://issues.apache.org/jira/browse/SPARK-7368) | New Feature | add QR decomposition for RowMatrix |
| 1.5.0 | [SPARK-7387](https://issues.apache.org/jira/browse/SPARK-7387) | New Feature | CrossValidator example code in Python |
| 1.5.0 | [SPARK-7422](https://issues.apache.org/jira/browse/SPARK-7422) | New Feature | Add argmax to Vector, SparseVector |
| 1.5.0 | [SPARK-7423](https://issues.apache.org/jira/browse/SPARK-7423) | Improvement | spark.ml Classifier predict should not convert vectors to dense format |
| 1.5.0 | [SPARK-7426](https://issues.apache.org/jira/browse/SPARK-7426) | Improvement | spark.ml AttributeFactory.fromStructField should allow other NumericTypes |
| 1.5.0 | [SPARK-7446](https://issues.apache.org/jira/browse/SPARK-7446) | Improvement | Inverse transform for StringIndexer |
| 1.5.0 | [SPARK-7604](https://issues.apache.org/jira/browse/SPARK-7604) | New Feature | Python API for PCA and PCAModel |
| 1.5.0 | [SPARK-7605](https://issues.apache.org/jira/browse/SPARK-7605) | New Feature | Python API for ElementwiseProduct |
| 1.5.0 | [SPARK-7663](https://issues.apache.org/jira/browse/SPARK-7663) | Improvement | [MLLIB] feature.Word2Vec throws empty iterator error when the vocabulary size is zero |
| 1.5.0 | [SPARK-7690](https://issues.apache.org/jira/browse/SPARK-7690) | New Feature | MulticlassClassificationEvaluator for tuning Multiclass Classifiers |
| 1.5.0 | [SPARK-7739](https://issues.apache.org/jira/browse/SPARK-7739) | Improvement | Improve ChiSqSelector example code in the user guide |
| 1.5.0 | [SPARK-7879](https://issues.apache.org/jira/browse/SPARK-7879) | New Feature | KMeans API for spark.ml Pipelines |
| 1.5.0 | [SPARK-7888](https://issues.apache.org/jira/browse/SPARK-7888) | New Feature | Be able to disable intercept in Linear Regression in ML package |
| 1.5.0 | [SPARK-7916](https://issues.apache.org/jira/browse/SPARK-7916) | Improvement | MLlib Python doc parity check for classification and regression. |
| 1.5.0 | [SPARK-8018](https://issues.apache.org/jira/browse/SPARK-8018) | Improvement | KMeans should accept initial cluster centers as param |
| 1.5.0 | [SPARK-8054](https://issues.apache.org/jira/browse/SPARK-8054) | Improvement | Java compatibility fixes for MLlib 1.4 |
| 1.5.0 | [SPARK-8068](https://issues.apache.org/jira/browse/SPARK-8068) | Improvement | Add confusionMatrix method at class MulticlassMetrics in pyspark/mllib |
| 1.5.0 | [SPARK-8069](https://issues.apache.org/jira/browse/SPARK-8069) | Improvement | Add support for cutoff to RandomForestClassifier |
| 1.5.0 | [SPARK-8169](https://issues.apache.org/jira/browse/SPARK-8169) | New Feature | Add StopWordsRemover as a transformer |
| 1.5.0 | [SPARK-8265](https://issues.apache.org/jira/browse/SPARK-8265) | Improvement | Add LinearDataGenerator to pyspark.mllib.utils |
| 1.5.0 | [SPARK-8445](https://issues.apache.org/jira/browse/SPARK-8445) | Umbrella | MLlib 1.5 Roadmap |
| 1.5.0 | [SPARK-8456](https://issues.apache.org/jira/browse/SPARK-8456) | New Feature | Python API for N-Gram Feature Transformer |
| 1.5.0 | [SPARK-8481](https://issues.apache.org/jira/browse/SPARK-8481) | Improvement | GaussianMixtureModel predict accepting single vector |
| 1.5.0 | [SPARK-8484](https://issues.apache.org/jira/browse/SPARK-8484) | New Feature | Add TrainValidationSplit to ml.tuning |
| 1.5.0 | [SPARK-8521](https://issues.apache.org/jira/browse/SPARK-8521) | Umbrella | Feature Transformers in 1.5 |
| 1.5.0 | [SPARK-8522](https://issues.apache.org/jira/browse/SPARK-8522) | New Feature | Disable feature scaling in Linear and Logistic Regression |
| 1.5.0 | [SPARK-8536](https://issues.apache.org/jira/browse/SPARK-8536) | Improvement | Generalize LDA to asymmetric doc-topic priors |
| 1.5.0 | [SPARK-8538](https://issues.apache.org/jira/browse/SPARK-8538) | New Feature | LinearRegressionResults class for storing LR results on data |
| 1.5.0 | [SPARK-8539](https://issues.apache.org/jira/browse/SPARK-8539) | New Feature | LinearRegressionSummary class for storing LR training stats |
| 1.5.0 | [SPARK-8559](https://issues.apache.org/jira/browse/SPARK-8559) | Improvement | Support association rule generation in FPGrowth |
| 1.5.0 | [SPARK-8570](https://issues.apache.org/jira/browse/SPARK-8570) | Improvement | Improve MLlib Local Matrix Documentation. |
| 1.5.0 | [SPARK-8600](https://issues.apache.org/jira/browse/SPARK-8600) | New Feature | Naive Bayes API for spark.ml Pipelines |
| 1.5.0 | [SPARK-8660](https://issues.apache.org/jira/browse/SPARK-8660) | Improvement | Update comments that contain R statements in ml.logisticRegressionSuite |
| 1.5.0 | [SPARK-8661](https://issues.apache.org/jira/browse/SPARK-8661) | Improvement | Update comments that contain R statements in ml.LinearRegressionSuite |
| 1.5.0 | [SPARK-8671](https://issues.apache.org/jira/browse/SPARK-8671) | New Feature | Add isotonic regression to the pipeline API |
| 1.5.0 | [SPARK-8704](https://issues.apache.org/jira/browse/SPARK-8704) | New Feature | Add missing methods in StandardScaler (ML and PySpark) |
| 1.5.0 | [SPARK-8744](https://issues.apache.org/jira/browse/SPARK-8744) | Improvement | StringIndexerModel should have public constructor |
| 1.5.0 | [SPARK-8757](https://issues.apache.org/jira/browse/SPARK-8757) | Umbrella | Check missing and add user guide for MLlib Python API |
| 1.5.0 | [SPARK-8774](https://issues.apache.org/jira/browse/SPARK-8774) | New Feature | Add R model formula with basic support as a transformer |
| 1.5.0 | [SPARK-8788](https://issues.apache.org/jira/browse/SPARK-8788) | Improvement | Java unit test for PCA transformer |
| 1.5.0 | [SPARK-8792](https://issues.apache.org/jira/browse/SPARK-8792) | Improvement | Add Python API for PCA transformer |
| 1.5.0 | [SPARK-8823](https://issues.apache.org/jira/browse/SPARK-8823) | Improvement | Optimizations for sparse vector products in pyspark.mllib.linalg |
| 1.5.0 | [SPARK-8872](https://issues.apache.org/jira/browse/SPARK-8872) | Improvement | Improve FPGrowthSuite with equivalent R code |
| 1.5.0 | [SPARK-8874](https://issues.apache.org/jira/browse/SPARK-8874) | New Feature | Add missing methods in Word2Vec ML |
| 1.5.0 | [SPARK-8877](https://issues.apache.org/jira/browse/SPARK-8877) | Improvement | Public API for association rule generation |
| 1.5.0 | [SPARK-8936](https://issues.apache.org/jira/browse/SPARK-8936) | New Feature | Hyperparameter estimation in LDA |
| 1.5.0 | [SPARK-8963](https://issues.apache.org/jira/browse/SPARK-8963) | Improvement | Improve Linear Regression tests to use Vectors |
| 1.5.0 | [SPARK-8997](https://issues.apache.org/jira/browse/SPARK-8997) | Improvement | Improve LocalPrefixSpan performance |
| 1.5.0 | [SPARK-8998](https://issues.apache.org/jira/browse/SPARK-8998) | Improvement | Collect enough frequent prefixes before projection in PrefixSpan |
| 1.5.0 | [SPARK-8999](https://issues.apache.org/jira/browse/SPARK-8999) | Improvement | Support non-temporal sequence in PrefixSpan |
| 1.5.0 | [SPARK-9000](https://issues.apache.org/jira/browse/SPARK-9000) | Improvement | Support generic item type in PrefixSpan |
| 1.5.0 | [SPARK-9028](https://issues.apache.org/jira/browse/SPARK-9028) | New Feature | Add CountVectorizer as an estimator to generate CountVectorizerModel |
| 1.5.0 | [SPARK-9077](https://issues.apache.org/jira/browse/SPARK-9077) | Improvement | Improve error message for decision trees when numExamples < maxCategoriesPerFeature |
| 1.5.0 | [SPARK-9112](https://issues.apache.org/jira/browse/SPARK-9112) | New Feature | Implement LogisticRegressionSummary similar to LinearRegressionSummary |
| 1.5.0 | [SPARK-9118](https://issues.apache.org/jira/browse/SPARK-9118) | Improvement | Implement integer array parameters for ml.param as IntArrayParam |
| 1.5.0 | [SPARK-9122](https://issues.apache.org/jira/browse/SPARK-9122) | Improvement | spark.mllib regression should support batch predict |
| 1.5.0 | [SPARK-9191](https://issues.apache.org/jira/browse/SPARK-9191) | Improvement | Add ml.PCA user guide and code examples |
| 1.5.0 | [SPARK-9214](https://issues.apache.org/jira/browse/SPARK-9214) | Improvement | support ml.NaiveBayes for Python |
| 1.5.0 | [SPARK-9231](https://issues.apache.org/jira/browse/SPARK-9231) | New Feature | DistributedLDAModel method for top topics per document |
| 1.5.0 | [SPARK-9245](https://issues.apache.org/jira/browse/SPARK-9245) | New Feature | DistributedLDAModel predict top topic per doc-term instance |
| 1.5.0 | [SPARK-9246](https://issues.apache.org/jira/browse/SPARK-9246) | New Feature | DistributedLDAModel predict top docs per topic |
| 1.5.0 | [SPARK-9308](https://issues.apache.org/jira/browse/SPARK-9308) | Improvement | ml.NaiveBayesModel support predicting class probabilities |
| 1.5.0 | [SPARK-9337](https://issues.apache.org/jira/browse/SPARK-9337) | Improvement | Add an ut for Word2Vec to verify the empty vocabulary check |
| 1.5.0 | [SPARK-9376](https://issues.apache.org/jira/browse/SPARK-9376) | Improvement | use a seed in RandomDataGeneratorSuite |
| 1.5.0 | [SPARK-9440](https://issues.apache.org/jira/browse/SPARK-9440) | New Feature | LocalLDAModel should save docConcentration, topicConcentration, and gammaShape |
| 1.5.0 | [SPARK-9447](https://issues.apache.org/jira/browse/SPARK-9447) | Improvement | Python RandomForestClassifier probabilityCol, rawPredictionCol |
| 1.5.0 | [SPARK-9454](https://issues.apache.org/jira/browse/SPARK-9454) | Improvement | LDASuite should use vector comparisons |
| 1.5.0 | [SPARK-9471](https://issues.apache.org/jira/browse/SPARK-9471) | New Feature | Multilayer perceptron classifier |
| 1.5.0 | [SPARK-9481](https://issues.apache.org/jira/browse/SPARK-9481) | Improvement | LocalLDAModel logLikelihood |
| 1.5.0 | [SPARK-9493](https://issues.apache.org/jira/browse/SPARK-9493) | Improvement | Chain logistic regression with isotonic regression under the pipeline API |
| 1.5.0 | [SPARK-9527](https://issues.apache.org/jira/browse/SPARK-9527) | Improvement | PrefixSpan.run should return a PrefixSpanModel instead of an RDD and it should be Java-friendly |
| 1.5.0 | [SPARK-9528](https://issues.apache.org/jira/browse/SPARK-9528) | Improvement | RandomForestClassifier should extend ProbabilisticClassifier |
| 1.5.0 | [SPARK-9533](https://issues.apache.org/jira/browse/SPARK-9533) | Improvement | Add missing methods in Word2Vec ML (Python API) |
| 1.5.0 | [SPARK-9536](https://issues.apache.org/jira/browse/SPARK-9536) | Improvement | NaiveBayesModel support probability prediction for PySpark.ml |
| 1.5.0 | [SPARK-9537](https://issues.apache.org/jira/browse/SPARK-9537) | Improvement | DecisionTreeClassifierModel support probability prediction for PySpark.ml |
| 1.5.0 | [SPARK-9538](https://issues.apache.org/jira/browse/SPARK-9538) | Improvement | LogisticRegression support raw and probability prediction for PySpark.ml |
| 1.5.0 | [SPARK-9540](https://issues.apache.org/jira/browse/SPARK-9540) | Improvement | Optimize PrefixSpan implementation |
| 1.5.0 | [SPARK-9544](https://issues.apache.org/jira/browse/SPARK-9544) | New Feature | RFormula in Python |
| 1.5.0 | [SPARK-9568](https://issues.apache.org/jira/browse/SPARK-9568) | Umbrella | Spark MLlib 1.5.0 testing umbrella |
| 1.5.0 | [SPARK-9582](https://issues.apache.org/jira/browse/SPARK-9582) | Improvement | LDA cleanups |
| 1.5.0 | [SPARK-9586](https://issues.apache.org/jira/browse/SPARK-9586) | Improvement | Update BinaryClassificationEvaluator to use setRawPredictionCol |
| 1.5.0 | [SPARK-9657](https://issues.apache.org/jira/browse/SPARK-9657) | New Feature | PrefixSpan getMaxPatternLength should return an Int |
| 1.5.0 | [SPARK-9704](https://issues.apache.org/jira/browse/SPARK-9704) | Improvement | Make some ML APIs public: VectorUDT, Identifiable, ProbabilisticClassifier |
| 1.5.0 | [SPARK-9756](https://issues.apache.org/jira/browse/SPARK-9756) | Improvement | Make auxillary constructors for ML decision trees private |
| 1.5.0 | [SPARK-9768](https://issues.apache.org/jira/browse/SPARK-9768) | Improvement | Add Python API for ml.feature.ElementwiseProduct |
| 1.5.0 | [SPARK-9788](https://issues.apache.org/jira/browse/SPARK-9788) | Improvement | LDA docConcentration, gammaShape 1.5 binary incompatibility fixes |
| 1.5.0 | [SPARK-9789](https://issues.apache.org/jira/browse/SPARK-9789) | Improvement | Reinstate LogisticRegression threshold Param |
| 1.5.0 | [SPARK-9847](https://issues.apache.org/jira/browse/SPARK-9847) | Improvement | ML Params copyValues should copy default values to default map, not set map |
| 1.5.0 | [SPARK-9903](https://issues.apache.org/jira/browse/SPARK-9903) | Improvement | Skip local processing in PrefixSpan if there are no small prefixes |
| 1.5.0 | [SPARK-9909](https://issues.apache.org/jira/browse/SPARK-9909) | Improvement | Move weightCol to sharedParams |
| 1.5.0 | [SPARK-9913](https://issues.apache.org/jira/browse/SPARK-9913) | Improvement | LDAUtils should be private |
| 1.5.0 | [SPARK-9914](https://issues.apache.org/jira/browse/SPARK-9914) | Improvement | RFormula are missing setters for featuresCol and labelCol |
| 1.5.0 | [SPARK-9915](https://issues.apache.org/jira/browse/SPARK-9915) | Improvement | StopWordsRemover.stopWords should use StringArrayParam |
| 1.5.0 | [SPARK-9917](https://issues.apache.org/jira/browse/SPARK-9917) | Improvement | MinMaxScaler missing getters and docs |
| 1.5.0 | [SPARK-9918](https://issues.apache.org/jira/browse/SPARK-9918) | Improvement | Remove runs from KMeans under the pipeline API |
| 1.5.0 | [SPARK-9922](https://issues.apache.org/jira/browse/SPARK-9922) | Improvement | Rename StringIndexerInverse to IndexToString |
| 1.5.0 | [SPARK-9977](https://issues.apache.org/jira/browse/SPARK-9977) | Improvement | The usage of a label generated by StringIndexer |
| 1.5.0 | [SPARK-9981](https://issues.apache.org/jira/browse/SPARK-9981) | Improvement | Make labels public in StringIndexerModel |
| 1.5.0 | [SPARK-10068](https://issues.apache.org/jira/browse/SPARK-10068) | Improvement | Add links to sections in MLlib's user guide |
| 1.5.0 | [SPARK-10076](https://issues.apache.org/jira/browse/SPARK-10076) | Improvement | make MultilayerPerceptronClassifier layers and weights public |
| 1.5.0 | [SPARK-10085](https://issues.apache.org/jira/browse/SPARK-10085) | Improvement | unnecessary array import in Python MLLib linear models |
| 1.5.0 | [SPARK-10097](https://issues.apache.org/jira/browse/SPARK-10097) | Improvement | ML Evaluator should indicate if metric should be maximized or minimized |
| 1.5.0 | [SPARK-10163](https://issues.apache.org/jira/browse/SPARK-10163) | Improvement | Allow single-category features for GBT models |
| 1.5.0 | [SPARK-10230](https://issues.apache.org/jira/browse/SPARK-10230) | Improvement | LDA public API should use docConcentration |
| 1.5.0 | [SPARK-10354](https://issues.apache.org/jira/browse/SPARK-10354) | Improvement | First cost RDD shouldn't be cached in k-means\|\| and the following cost RDD should use MEMORY_AND_DISK |
| 1.5.0 | [SPARK-10729](https://issues.apache.org/jira/browse/SPARK-10729) | Improvement | word2vec model save for python |
| 1.6.0 | [SPARK-5565](https://issues.apache.org/jira/browse/SPARK-5565) | New Feature | LDA wrapper for spark.ml package |
| 1.6.0 | [SPARK-6517](https://issues.apache.org/jira/browse/SPARK-6517) | New Feature | Bisecting k-means clustering |
| 1.6.0 | [SPARK-7685](https://issues.apache.org/jira/browse/SPARK-7685) | New Feature | Handle high imbalanced data and apply weights to different samples in Logistic Regression |
| 1.6.0 | [SPARK-7770](https://issues.apache.org/jira/browse/SPARK-7770) | Improvement | Change GBT validationTol to be relative tolerance |
| 1.6.0 | [SPARK-8467](https://issues.apache.org/jira/browse/SPARK-8467) | New Feature | Add LDAModel.describeTopics() in Python |
| 1.6.0 | [SPARK-8518](https://issues.apache.org/jira/browse/SPARK-8518) | New Feature | Log-linear models for survival analysis |
| 1.6.0 | [SPARK-8530](https://issues.apache.org/jira/browse/SPARK-8530) | Improvement | Add Python API for MinMaxScaler |
| 1.6.0 | [SPARK-8764](https://issues.apache.org/jira/browse/SPARK-8764) | Improvement | StringIndexer should take option to handle unseen values |
| 1.6.0 | [SPARK-9570](https://issues.apache.org/jira/browse/SPARK-9570) | Improvement | Consistent recommendation for submitting spark apps to YARN, -master yarn --deploy-mode x vs -master yarn-x'. |
| 1.6.0 | [SPARK-9642](https://issues.apache.org/jira/browse/SPARK-9642) | New Feature | LinearRegression should supported weighted data |
| 1.6.0 | [SPARK-9654](https://issues.apache.org/jira/browse/SPARK-9654) | New Feature | Add IndexToString in Pyspark |
| 1.6.0 | [SPARK-9679](https://issues.apache.org/jira/browse/SPARK-9679) | New Feature | Add python interface for ml.feature.StopWordsRemover |
| 1.6.0 | [SPARK-9698](https://issues.apache.org/jira/browse/SPARK-9698) | New Feature | Add feature interaction as a transformer |
| 1.6.0 | [SPARK-9718](https://issues.apache.org/jira/browse/SPARK-9718) | Improvement | LinearRegressionTrainingSummary should hold all columns in transformed data |
| 1.6.0 | [SPARK-9720](https://issues.apache.org/jira/browse/SPARK-9720) | Improvement | spark.ml Identifiable types should have UID in toString methods |
| 1.6.0 | [SPARK-9722](https://issues.apache.org/jira/browse/SPARK-9722) | Improvement | Pass random seed to spark.ml RandomForest findSplitsBins |
| 1.6.0 | [SPARK-9723](https://issues.apache.org/jira/browse/SPARK-9723) | Improvement | Params.getOrDefault should throw more meaningful exception |
| 1.6.0 | [SPARK-9769](https://issues.apache.org/jira/browse/SPARK-9769) | New Feature | Add Python API for ml.feature.CountVectorizer |
| 1.6.0 | [SPARK-9772](https://issues.apache.org/jira/browse/SPARK-9772) | Improvement | Add Python API for ml.feature.VectorSlicer |
| 1.6.0 | [SPARK-9773](https://issues.apache.org/jira/browse/SPARK-9773) | Improvement | Add Python API for MultilayerPerceptronClassifier |
| 1.6.0 | [SPARK-9774](https://issues.apache.org/jira/browse/SPARK-9774) | New Feature | Add Python API for ml.regression.IsotonicRegression |
| 1.6.0 | [SPARK-9834](https://issues.apache.org/jira/browse/SPARK-9834) | New Feature | Normal equation solver for ordinary least squares |
| 1.6.0 | [SPARK-9841](https://issues.apache.org/jira/browse/SPARK-9841) | Improvement | Params.clear needs to be public |
| 1.6.0 | [SPARK-9930](https://issues.apache.org/jira/browse/SPARK-9930) | Umbrella | Feature transformers in 1.6 |
| 1.6.0 | [SPARK-9962](https://issues.apache.org/jira/browse/SPARK-9962) | Improvement | Decision Tree training: prevNodeIdsForInstances.unpersist() at end of training |
| 1.6.0 | [SPARK-9963](https://issues.apache.org/jira/browse/SPARK-9963) | Improvement | ML RandomForest cleanup: Move predictNodeIndex to LearningNode |
| 1.6.0 | [SPARK-10028](https://issues.apache.org/jira/browse/SPARK-10028) | Improvement | Add Python API for PrefixSpan |
| 1.6.0 | [SPARK-10064](https://issues.apache.org/jira/browse/SPARK-10064) | Improvement | Decision tree continuous feature binning is slow in large feature spaces |
| 1.6.0 | [SPARK-10194](https://issues.apache.org/jira/browse/SPARK-10194) | New Feature | SGD algorithms need convergenceTol parameter in Python |
| 1.6.0 | [SPARK-10249](https://issues.apache.org/jira/browse/SPARK-10249) | Improvement | Add Python Code Example to StopWordsRemover User Guide |
| 1.6.0 | [SPARK-10253](https://issues.apache.org/jira/browse/SPARK-10253) | Improvement | Remove Guava dependencies in MLlib java tests |
| 1.6.0 | [SPARK-10254](https://issues.apache.org/jira/browse/SPARK-10254) | Improvement | Remove Guava dependencies in spark.ml.feature |
| 1.6.0 | [SPARK-10255](https://issues.apache.org/jira/browse/SPARK-10255) | Improvement | Remove Guava dependencies in spark.ml.param |
| 1.6.0 | [SPARK-10256](https://issues.apache.org/jira/browse/SPARK-10256) | Improvement | Remove Guava dependencies in spark.ml.classificaiton |
| 1.6.0 | [SPARK-10257](https://issues.apache.org/jira/browse/SPARK-10257) | Improvement | Remove Guava dependencies in spark.mllib JavaTests |
| 1.6.0 | [SPARK-10299](https://issues.apache.org/jira/browse/SPARK-10299) | Improvement | word2vec should allow users to specify the window size |
| 1.6.0 | [SPARK-10324](https://issues.apache.org/jira/browse/SPARK-10324) | Umbrella | MLlib 1.6 Roadmap |
| 1.6.0 | [SPARK-10349](https://issues.apache.org/jira/browse/SPARK-10349) | Improvement | OneVsRest use "when ... otherwise" not UDF to generate new label at binary reduction |
| 1.6.0 | [SPARK-10355](https://issues.apache.org/jira/browse/SPARK-10355) | Improvement | Add Python API for SQLTransformer |
| 1.6.0 | [SPARK-10385](https://issues.apache.org/jira/browse/SPARK-10385) | prose | Univariate and bivariate statistics in DataFrames |
| 1.6.0 | [SPARK-10393](https://issues.apache.org/jira/browse/SPARK-10393) | Improvement | use ML pipeline in LDA example |
| 1.6.0 | [SPARK-10394](https://issues.apache.org/jira/browse/SPARK-10394) | Improvement | Make GBTParams use shared "stepSize" |
| 1.6.0 | [SPARK-10464](https://issues.apache.org/jira/browse/SPARK-10464) | Improvement | Add WeibullGenerator for RandomDataGenerator |
| 1.6.0 | [SPARK-10490](https://issues.apache.org/jira/browse/SPARK-10490) | Improvement | Consolidate the Cholesky solvers in WeightedLeastSquares and ALS |
| 1.6.0 | [SPARK-10491](https://issues.apache.org/jira/browse/SPARK-10491) | Improvement | move RowMatrix.dspr to BLAS |
| 1.6.0 | [SPARK-10516](https://issues.apache.org/jira/browse/SPARK-10516) | New Feature | Add values as a property to DenseVector in PySpark |
| 1.6.0 | [SPARK-10518](https://issues.apache.org/jira/browse/SPARK-10518) | Improvement | Update code examples in spark.ml user guide to use LIBSVM data source instead of MLUtils |
| 1.6.0 | [SPARK-10592](https://issues.apache.org/jira/browse/SPARK-10592) | Improvement | deprecate weights and use coefficients instead in ML models |
| 1.6.0 | [SPARK-10599](https://issues.apache.org/jira/browse/SPARK-10599) | Improvement | Decrease communication in BlockMatrix multiply and increase performance |
| 1.6.0 | [SPARK-10626](https://issues.apache.org/jira/browse/SPARK-10626) | Improvement | Create a Java friendly method for randomRDD & RandomDataGenerator on RandomRDDs. |
| 1.6.0 | [SPARK-10654](https://issues.apache.org/jira/browse/SPARK-10654) | Improvement | Add columnSimilarities to IndexedRowMatrix |
| 1.6.0 | [SPARK-10668](https://issues.apache.org/jira/browse/SPARK-10668) | New Feature | Use WeightedLeastSquares in LinearRegression with L2 regularization if the number of features is small |
| 1.6.0 | [SPARK-10686](https://issues.apache.org/jira/browse/SPARK-10686) | New Feature | Add quantileCol to AFTSurvivalRegression |
| 1.6.0 | [SPARK-10688](https://issues.apache.org/jira/browse/SPARK-10688) | New Feature | Python API for AFTSurvivalRegression |
| 1.6.0 | [SPARK-10715](https://issues.apache.org/jira/browse/SPARK-10715) | Improvement | Duplicate initialzation flag in WeightedLeastSquare |
| 1.6.0 | [SPARK-10738](https://issues.apache.org/jira/browse/SPARK-10738) | Improvement | Refactoring `Instance` out from LOR and LIR, and also cleaning up some code |
| 1.6.0 | [SPARK-10778](https://issues.apache.org/jira/browse/SPARK-10778) | New Feature | Implement toString for AssociationRules.Rule |
| 1.6.0 | [SPARK-10779](https://issues.apache.org/jira/browse/SPARK-10779) | New Feature | Set initialModel for KMeans model in PySpark (spark.mllib) |
| 1.6.0 | [SPARK-11029](https://issues.apache.org/jira/browse/SPARK-11029) | New Feature | Add computeCost to KMeansModel in spark.ml |
| 1.6.0 | [SPARK-11050](https://issues.apache.org/jira/browse/SPARK-11050) | Improvement | PySpark SparseVector can return wrong index in error message |
| 1.6.0 | [SPARK-11069](https://issues.apache.org/jira/browse/SPARK-11069) | New Feature | Add RegexTokenizer option to convert to lowercase |
| 1.6.0 | [SPARK-11084](https://issues.apache.org/jira/browse/SPARK-11084) | Improvement | SparseVector.__getitem__ should check if value can be non-zero before executing searchsorted |
| 1.6.0 | [SPARK-11184](https://issues.apache.org/jira/browse/SPARK-11184) | Improvement | Declare most of .mllib code not-Experimental |
| 1.6.0 | [SPARK-11207](https://issues.apache.org/jira/browse/SPARK-11207) | Improvement | Add test cases for solver selection of LinearRegression as followup. |
| 1.6.0 | [SPARK-11332](https://issues.apache.org/jira/browse/SPARK-11332) | Improvement | WeightedLeastSquares should use ml features generic Instance class instead of private |
| 1.6.0 | [SPARK-11358](https://issues.apache.org/jira/browse/SPARK-11358) | Improvement | Deprecate `runs` in k-means |
| 1.6.0 | [SPARK-11367](https://issues.apache.org/jira/browse/SPARK-11367) | Improvement | Python LinearRegression should support setting solver |
| 1.6.0 | [SPARK-11385](https://issues.apache.org/jira/browse/SPARK-11385) | Improvement | Make foreachActive public in MLLib's vector API |
| 1.6.0 | [SPARK-11488](https://issues.apache.org/jira/browse/SPARK-11488) | Improvement | GroupedData should only keep common first order statistics |
| 1.6.0 | [SPARK-11489](https://issues.apache.org/jira/browse/SPARK-11489) | Improvement | Only include common first order statistics in GroupedData |
| 1.6.0 | [SPARK-11514](https://issues.apache.org/jira/browse/SPARK-11514) | New Feature | Pass random seed to spark.ml DecisionTree* |
| 1.6.0 | [SPARK-11527](https://issues.apache.org/jira/browse/SPARK-11527) | Improvement | PySpark AFTSurvivalRegressionModel should expose coefficients/intercept/scale |
| 1.6.0 | [SPARK-11566](https://issues.apache.org/jira/browse/SPARK-11566) | Improvement | Refactoring GaussianMixtureModel.gaussians in Python |
| 1.6.0 | [SPARK-11600](https://issues.apache.org/jira/browse/SPARK-11600) | Umbrella | Spark MLlib 1.6 QA umbrella |
| 1.6.0 | [SPARK-11629](https://issues.apache.org/jira/browse/SPARK-11629) | Improvement | Python example code for Multilayer Perceptron Classification |
| 1.6.0 | [SPARK-11813](https://issues.apache.org/jira/browse/SPARK-11813) | Improvement | Avoid serialization of vocab in Word2Vec |
| 1.6.0 | [SPARK-11816](https://issues.apache.org/jira/browse/SPARK-11816) | Improvement | fix some style issue in ML/MLlib examples |
| 1.6.0 | [SPARK-11835](https://issues.apache.org/jira/browse/SPARK-11835) | Improvement | Add a menu to the documentation of MLlib |
| 1.6.0 | [SPARK-11852](https://issues.apache.org/jira/browse/SPARK-11852) | Improvement | StandardScaler minor refactor |
| 1.6.0 | [SPARK-11895](https://issues.apache.org/jira/browse/SPARK-11895) | Improvement | Rename and possibly update DatasetExample in mllib/examples |
| 1.6.0 | [SPARK-11902](https://issues.apache.org/jira/browse/SPARK-11902) | Improvement | Unhandled case in VectorAssembler#transform |
| 1.6.0 | [SPARK-11912](https://issues.apache.org/jira/browse/SPARK-11912) | Improvement | ml.feature.PCA minor refactor |
| 1.6.0 | [SPARK-11920](https://issues.apache.org/jira/browse/SPARK-11920) | Improvement | ML LinearRegression should use correct dataset in examples and user guide doc |
| 2.0.0 | — | prose | SparkR gains MLlib APIs for GLM, naive Bayes, k-means, survival regression |
| 2.0.0 | — | prose | PySpark gains many more MLlib algorithms (LDA, GMM, GLR) |
| 2.0.0 | — | prose | New DataFrame-based algorithms: Bisecting K-Means, GMM, MaxAbsScaler |
| 2.0.0 | [SPARK-3724](https://issues.apache.org/jira/browse/SPARK-3724) | Improvement | RandomForest: More options for feature subset size |
| 2.0.0 | [SPARK-5273](https://issues.apache.org/jira/browse/SPARK-5273) | Improvement | Improve documentation examples for LinearRegression |
| 2.0.0 | [SPARK-5991](https://issues.apache.org/jira/browse/SPARK-5991) | Umbrella | Python API for ML model import/export |
| 2.0.0 | [SPARK-6519](https://issues.apache.org/jira/browse/SPARK-6519) | New Feature | Add spark.ml API for bisecting k-means |
| 2.0.0 | [SPARK-6717](https://issues.apache.org/jira/browse/SPARK-6717) | Improvement | Clear shuffle files after checkpointing in ALS |
| 2.0.0 | [SPARK-7617](https://issues.apache.org/jira/browse/SPARK-7617) | Improvement | Word2VecModel fVector not normalized |
| 2.0.0 | [SPARK-7675](https://issues.apache.org/jira/browse/SPARK-7675) | Improvement | PySpark spark.ml Params type conversions |
| 2.0.0 | [SPARK-7751](https://issues.apache.org/jira/browse/SPARK-7751) | Umbrella | Add @Since annotation to stable and experimental methods in MLlib |
| 2.0.0 | [SPARK-7861](https://issues.apache.org/jira/browse/SPARK-7861) | New Feature | Python wrapper for OneVsRest |
| 2.0.0 | [SPARK-9716](https://issues.apache.org/jira/browse/SPARK-9716) | Improvement | BinaryClassificationEvaluator should accept Double prediction column |
| 2.0.0 | [SPARK-9835](https://issues.apache.org/jira/browse/SPARK-9835) | New Feature | Iteratively reweighted least squares solver for GLMs |
| 2.0.0 | [SPARK-9837](https://issues.apache.org/jira/browse/SPARK-9837) | New Feature | Provide R-like summary statistics for GLMs via iteratively reweighted least squares |
| 2.0.0 | [SPARK-10158](https://issues.apache.org/jira/browse/SPARK-10158) | Improvement | ALS should print better errors when given Long IDs |
| 2.0.0 | [SPARK-10299](https://issues.apache.org/jira/browse/SPARK-10299) | Improvement | word2vec should allow users to specify the window size |
| 2.0.0 | [SPARK-10691](https://issues.apache.org/jira/browse/SPARK-10691) | Improvement | Make Logistic, Linear Regression Model evaluate() method public |
| 2.0.0 | [SPARK-10788](https://issues.apache.org/jira/browse/SPARK-10788) | Improvement | Decision Tree duplicates bins for unordered categorical features |
| 2.0.0 | [SPARK-10809](https://issues.apache.org/jira/browse/SPARK-10809) | New Feature | Single-document topicDistributions method for LocalLDAModel |
| 2.0.0 | [SPARK-10991](https://issues.apache.org/jira/browse/SPARK-10991) | Improvement | LogisticRegressionTrainingSummary should dynamically add prediction col if there is no prediction col set |
| 2.0.0 | [SPARK-11259](https://issues.apache.org/jira/browse/SPARK-11259) | Improvement | Params.validateParams() should be called automatically |
| 2.0.0 | [SPARK-11515](https://issues.apache.org/jira/browse/SPARK-11515) | New Feature | QuantileDiscretizer should take random seed |
| 2.0.0 | [SPARK-11530](https://issues.apache.org/jira/browse/SPARK-11530) | Improvement | Return eigenvalues with PCA model |
| 2.0.0 | [SPARK-11531](https://issues.apache.org/jira/browse/SPARK-11531) | Improvement | PySpark SparseVector: improve error message for bad indices |
| 2.0.0 | [SPARK-11559](https://issues.apache.org/jira/browse/SPARK-11559) | Improvement | Make `runs` no effect in k-means |
| 2.0.0 | [SPARK-11611](https://issues.apache.org/jira/browse/SPARK-11611) | New Feature | Python API for bisecting k-means |
| 2.0.0 | [SPARK-11730](https://issues.apache.org/jira/browse/SPARK-11730) | New Feature | Feature Importance for GBT |
| 2.0.0 | [SPARK-11826](https://issues.apache.org/jira/browse/SPARK-11826) | Improvement | Subtract BlockMatrix |
| 2.0.0 | [SPARK-11861](https://issues.apache.org/jira/browse/SPARK-11861) | New Feature | Expose feature importances API for decision trees |
| 2.0.0 | [SPARK-11898](https://issues.apache.org/jira/browse/SPARK-11898) | Improvement | Use broadcast for the global tables in Word2Vec |
| 2.0.0 | [SPARK-11988](https://issues.apache.org/jira/browse/SPARK-11988) | Improvement | Update JPMML to 1.2.7 |
| 2.0.0 | [SPARK-12096](https://issues.apache.org/jira/browse/SPARK-12096) | Improvement | remove the old constraint in word2vec |
| 2.0.0 | [SPARK-12182](https://issues.apache.org/jira/browse/SPARK-12182) | Improvement | Distributed binning for trees in spark.ml |
| 2.0.0 | [SPARK-12183](https://issues.apache.org/jira/browse/SPARK-12183) | Improvement | Remove spark.mllib tree, forest implementations and use spark.ml |
| 2.0.0 | [SPARK-12301](https://issues.apache.org/jira/browse/SPARK-12301) | Improvement | Remove final from classes in spark.ml trees and ensembles where possible |
| 2.0.0 | [SPARK-12309](https://issues.apache.org/jira/browse/SPARK-12309) | Improvement | Use sqlContext from MLlibTestSparkContext for spark.ml test suites |
| 2.0.0 | [SPARK-12331](https://issues.apache.org/jira/browse/SPARK-12331) | Improvement | R^2 for regression through the origin |
| 2.0.0 | [SPARK-12349](https://issues.apache.org/jira/browse/SPARK-12349) | Improvement | Make spark.ml PCAModel load backwards compatible |
| 2.0.0 | [SPARK-12368](https://issues.apache.org/jira/browse/SPARK-12368) | Improvement | Better doc for the binary classification evaluator' metricName |
| 2.0.0 | [SPARK-12450](https://issues.apache.org/jira/browse/SPARK-12450) | Improvement | Un-persist broadcasted variables in KMeans |
| 2.0.0 | [SPARK-12494](https://issues.apache.org/jira/browse/SPARK-12494) | Improvement | Array out of bound Exception in KMeans Yarn Mode |
| 2.0.0 | [SPARK-12569](https://issues.apache.org/jira/browse/SPARK-12569) | New Feature | DecisionTreeRegressor: provide variance of prediction: Python API |
| 2.0.0 | [SPARK-12599](https://issues.apache.org/jira/browse/SPARK-12599) | Improvement | Remove the use of the deprecated callUDF in MLlib |
| 2.0.0 | [SPARK-12603](https://issues.apache.org/jira/browse/SPARK-12603) | Improvement | PySpark MLlib GaussianMixtureModel should support single instance predict/predictSoft |
| 2.0.0 | [SPARK-12626](https://issues.apache.org/jira/browse/SPARK-12626) | Umbrella | MLlib 2.0 Roadmap |
| 2.0.0 | [SPARK-12810](https://issues.apache.org/jira/browse/SPARK-12810) | Improvement | PySpark CrossValidatorModel should support avgMetrics |
| 2.0.0 | [SPARK-12811](https://issues.apache.org/jira/browse/SPARK-12811) | New Feature | Estimator interface for generalized linear models (GLMs) |
| 2.0.0 | [SPARK-12869](https://issues.apache.org/jira/browse/SPARK-12869) | Improvement | Optimize conversion from BlockMatrix to IndexedRowMatrix |
| 2.0.0 | [SPARK-12877](https://issues.apache.org/jira/browse/SPARK-12877) | New Feature | TrainValidationSplit is missing in pyspark.ml.tuning |
| 2.0.0 | [SPARK-12908](https://issues.apache.org/jira/browse/SPARK-12908) | Improvement | Add tests to make sure that ml.classification.LogisticRegression returns meaningful result when labels are the same without intercept |
| 2.0.0 | [SPARK-12974](https://issues.apache.org/jira/browse/SPARK-12974) | Improvement | Add Python API for spark.ml bisecting k-means |
| 2.0.0 | [SPARK-13028](https://issues.apache.org/jira/browse/SPARK-13028) | New Feature | Add MaxAbsScaler to ML.feature as a transformer |
| 2.0.0 | [SPARK-13097](https://issues.apache.org/jira/browse/SPARK-13097) | Improvement | Extend Binarizer to allow Double AND Vector inputs |
| 2.0.0 | [SPARK-13132](https://issues.apache.org/jira/browse/SPARK-13132) | Improvement | LogisticRegression spends 35% of its time fetching the standardization parameter |
| 2.0.0 | [SPARK-13257](https://issues.apache.org/jira/browse/SPARK-13257) | Improvement | Refine naive Bayes example code |
| 2.0.0 | [SPARK-13292](https://issues.apache.org/jira/browse/SPARK-13292) | New Feature | QuantileDiscretizer should take random seed in PySpark |
| 2.0.0 | [SPARK-13295](https://issues.apache.org/jira/browse/SPARK-13295) | Improvement | ML/MLLIB: AFTSurvivalRegression: Improve AFTAggregator - Avoid creating new instances of arrays/vectors for each record |
| 2.0.0 | [SPARK-13322](https://issues.apache.org/jira/browse/SPARK-13322) | Improvement | AFTSurvivalRegression should support feature standardization |
| 2.0.0 | [SPARK-13429](https://issues.apache.org/jira/browse/SPARK-13429) | Improvement | Unify Logistic Regression convergence tolerance of ML & MLlib |
| 2.0.0 | [SPARK-13430](https://issues.apache.org/jira/browse/SPARK-13430) | Improvement | Expose ml summary function in PySpark for classification and regression models |
| 2.0.0 | [SPARK-13490](https://issues.apache.org/jira/browse/SPARK-13490) | Improvement | ML LinearRegression should cache standardization param value |
| 2.0.0 | [SPARK-13505](https://issues.apache.org/jira/browse/SPARK-13505) | New Feature | Python API for MaxAbsScaler |
| 2.0.0 | [SPARK-13545](https://issues.apache.org/jira/browse/SPARK-13545) | Improvement | Make MLlib LogisticRegressionWithLBFGS's default parameters consistent in Scala and Python |
| 2.0.0 | [SPARK-13550](https://issues.apache.org/jira/browse/SPARK-13550) | Improvement | Add java example for ml.clustering.BisectingKMeans |
| 2.0.0 | [SPARK-13551](https://issues.apache.org/jira/browse/SPARK-13551) | Improvement | Fix fix wrong comment and remove meanless lines in mllib.JavaBisectingKMeansExample |
| 2.0.0 | [SPARK-13590](https://issues.apache.org/jira/browse/SPARK-13590) | Improvement | Document the behavior of spark.ml logistic regression and AFT survival regression when there are constant features |
| 2.0.0 | [SPARK-13597](https://issues.apache.org/jira/browse/SPARK-13597) | New Feature | Python API for GeneralizedLinearRegression |
| 2.0.0 | [SPARK-13615](https://issues.apache.org/jira/browse/SPARK-13615) | Improvement | GeneralizedLinearRegression support save/load |
| 2.0.0 | [SPARK-13629](https://issues.apache.org/jira/browse/SPARK-13629) | New Feature | Add binary toggle Param to CountVectorizer |
| 2.0.0 | [SPARK-13646](https://issues.apache.org/jira/browse/SPARK-13646) | Improvement | QuantileDiscretizer counts dataset twice in getSampledInput |
| 2.0.0 | [SPARK-13672](https://issues.apache.org/jira/browse/SPARK-13672) | Improvement | Add python examples of BisectingKMeans in ML and MLLIB |
| 2.0.0 | [SPARK-13785](https://issues.apache.org/jira/browse/SPARK-13785) | Improvement | Deprecate model field in ML model summary classes |
| 2.0.0 | [SPARK-13787](https://issues.apache.org/jira/browse/SPARK-13787) | New Feature | Feature importances for decision trees in Python |
| 2.0.0 | [SPARK-13967](https://issues.apache.org/jira/browse/SPARK-13967) | New Feature | Add binary toggle Param to PySpark CountVectorizer |
| 2.0.0 | [SPARK-14030](https://issues.apache.org/jira/browse/SPARK-14030) | Improvement | Add parameter check to several MLlib implementations |
| 2.0.0 | [SPARK-14095](https://issues.apache.org/jira/browse/SPARK-14095) | Improvement | LogisticRegression fails when a DataFrame has only a one-class label |
| 2.0.0 | [SPARK-14107](https://issues.apache.org/jira/browse/SPARK-14107) | Improvement | PySpark spark.ml GBT algs need seed Param |
| 2.0.0 | [SPARK-14164](https://issues.apache.org/jira/browse/SPARK-14164) | Improvement | Improve input layer validation of MultilayerPerceptronClassifier |
| 2.0.0 | [SPARK-14181](https://issues.apache.org/jira/browse/SPARK-14181) | Improvement | TrainValidationSplit should have HasSeed |
| 2.0.0 | [SPARK-14264](https://issues.apache.org/jira/browse/SPARK-14264) | New Feature | Add feature importances for GBTs in Pyspark |
| 2.0.0 | [SPARK-14284](https://issues.apache.org/jira/browse/SPARK-14284) | Improvement | Rename KMeansSummary.size to clusterSizes |
| 2.0.0 | [SPARK-14339](https://issues.apache.org/jira/browse/SPARK-14339) | Improvement | Add python examples for DCT,MinMaxScaler,MaxAbsScaler |
| 2.0.0 | [SPARK-14340](https://issues.apache.org/jira/browse/SPARK-14340) | Improvement | Add Scala Example and User DOC for ml.BisectingKMeans |
| 2.0.0 | [SPARK-14375](https://issues.apache.org/jira/browse/SPARK-14375) | Improvement | Unit test for spark.ml KMeansSummary |
| 2.0.0 | [SPARK-14386](https://issues.apache.org/jira/browse/SPARK-14386) | Improvement | spark.ml DecisionTreeModel abstraction should not be exposed |
| 2.0.0 | [SPARK-14392](https://issues.apache.org/jira/browse/SPARK-14392) | New Feature | CountVectorizer Estimator should include binary toggle Param |
| 2.0.0 | [SPARK-14412](https://issues.apache.org/jira/browse/SPARK-14412) | New Feature | spark.ml ALS prefered storage level Params |
| 2.0.0 | [SPARK-14420](https://issues.apache.org/jira/browse/SPARK-14420) | Improvement | keepLastCheckpoint Param for Python LDA with EM |
| 2.0.0 | [SPARK-14440](https://issues.apache.org/jira/browse/SPARK-14440) | Improvement | Remove PySpark ml.pipeline's specific Reader and Writer |
| 2.0.0 | [SPARK-14461](https://issues.apache.org/jira/browse/SPARK-14461) | New Feature | GLM training summaries should provide solver |
| 2.0.0 | [SPARK-14479](https://issues.apache.org/jira/browse/SPARK-14479) | Improvement | GLM supports output link prediction |
| 2.0.0 | [SPARK-14497](https://issues.apache.org/jira/browse/SPARK-14497) | Improvement | Use top instead of sortBy() to get top N frequent words as dict in CountVectorizer |
| 2.0.0 | [SPARK-14500](https://issues.apache.org/jira/browse/SPARK-14500) | New Feature | Accept Dataset[_] instead of DataFrame in MLlib APIs |
| 2.0.0 | [SPARK-14509](https://issues.apache.org/jira/browse/SPARK-14509) | Improvement | Add python CountVectorizerExample |
| 2.0.0 | [SPARK-14510](https://issues.apache.org/jira/browse/SPARK-14510) | Improvement | Add args-checking for LDA and StreamingKMeans |
| 2.0.0 | [SPARK-14512](https://issues.apache.org/jira/browse/SPARK-14512) | Improvement | Add python example for QuantileDiscretizer |
| 2.0.0 | [SPARK-14514](https://issues.apache.org/jira/browse/SPARK-14514) | Improvement | Add python example for VectorSlicer |
| 2.0.0 | [SPARK-14564](https://issues.apache.org/jira/browse/SPARK-14564) | New Feature | Python Word2Vec missing setWindowSize method |
| 2.0.0 | [SPARK-14565](https://issues.apache.org/jira/browse/SPARK-14565) | Improvement | RandomForest should use parseInt and parseDouble for feature subset size instead of regexes |
| 2.0.0 | [SPARK-14605](https://issues.apache.org/jira/browse/SPARK-14605) | Improvement | Python spark.ml classes should use unicode uid |
| 2.0.0 | [SPARK-14646](https://issues.apache.org/jira/browse/SPARK-14646) | Improvement | k-means save/load should put one cluster per row |
| 2.0.0 | [SPARK-14829](https://issues.apache.org/jira/browse/SPARK-14829) | Improvement | Deprecate GLM APIs using SGD |
| 2.0.0 | [SPARK-14844](https://issues.apache.org/jira/browse/SPARK-14844) | Improvement | KMeansModel in spark.ml should allow to change featureCol and predictionCol |
| 2.0.0 | [SPARK-14850](https://issues.apache.org/jira/browse/SPARK-14850) | Improvement | VectorUDT/MatrixUDT should take primitive arrays without boxing |
| 2.0.0 | [SPARK-14862](https://issues.apache.org/jira/browse/SPARK-14862) | Improvement | Tree and ensemble classification: do not require label metadata |
| 2.0.0 | [SPARK-14899](https://issues.apache.org/jira/browse/SPARK-14899) | Improvement | Remove spark.ml HashingTF hashingAlg option |
| 2.0.0 | [SPARK-14900](https://issues.apache.org/jira/browse/SPARK-14900) | New Feature | spark.ml classification metrics should include accuracy |
| 2.0.0 | [SPARK-14903](https://issues.apache.org/jira/browse/SPARK-14903) | Improvement | Revert: Change MLWritable.write to be a property |
| 2.0.0 | [SPARK-14907](https://issues.apache.org/jira/browse/SPARK-14907) | Improvement | Use repartition in GLMRegressionModel.save |
| 2.0.0 | [SPARK-14916](https://issues.apache.org/jira/browse/SPARK-14916) | Improvement | A more friendly tostring for FreqItemset in mllib.fpm |
| 2.0.0 | [SPARK-14969](https://issues.apache.org/jira/browse/SPARK-14969) | Improvement | Remove unnecessary compute function in LogisticGradient |
| 2.0.0 | [SPARK-14971](https://issues.apache.org/jira/browse/SPARK-14971) | Improvement | PySpark ML Params setter code clean up |
| 2.0.0 | [SPARK-14978](https://issues.apache.org/jira/browse/SPARK-14978) | Improvement | PySpark TrainValidationSplitModel should support validationMetrics |
| 2.0.0 | [SPARK-14979](https://issues.apache.org/jira/browse/SPARK-14979) | Improvement | Add examples for GeneralizedLinearRegression |
| 2.0.0 | [SPARK-15106](https://issues.apache.org/jira/browse/SPARK-15106) | Improvement | Add package documentation for ML and remove BETA from Scala & Java for ML pipeline API. |
| 2.0.0 | [SPARK-15139](https://issues.apache.org/jira/browse/SPARK-15139) | Improvement | PySpark TreeEnsemble missing methods |
| 2.0.0 | [SPARK-15162](https://issues.apache.org/jira/browse/SPARK-15162) | Improvement | Update PySpark LogisticRegression threshold PyDoc to be as complete as Scaladoc |
| 2.0.0 | [SPARK-15168](https://issues.apache.org/jira/browse/SPARK-15168) | Improvement | Add missing params to Python's MultilayerPerceptronClassifier |
| 2.0.0 | [SPARK-15172](https://issues.apache.org/jira/browse/SPARK-15172) | Improvement | Warning message should explicitly tell user initial coefficients is ignored if its size doesn't match expected size in LogisticRegression |
| 2.0.0 | [SPARK-15181](https://issues.apache.org/jira/browse/SPARK-15181) | New Feature | Python API for Generalized Linear Regression Summary |
| 2.0.0 | [SPARK-15182](https://issues.apache.org/jira/browse/SPARK-15182) | Improvement | Copy MLlib doc to ML: ml.feature |
| 2.0.0 | [SPARK-15188](https://issues.apache.org/jira/browse/SPARK-15188) | Improvement | PySpark NaiveBayes is missing Thresholds param |
| 2.0.0 | [SPARK-15189](https://issues.apache.org/jira/browse/SPARK-15189) | Improvement | ml.Evaluation pydoc issues |
| 2.0.0 | [SPARK-15195](https://issues.apache.org/jira/browse/SPARK-15195) | Improvement | Improve PyDoc for ml.tuning |
| 2.0.0 | [SPARK-15281](https://issues.apache.org/jira/browse/SPARK-15281) | Improvement | PySpark ML GBTRegressor lacks impurity param |
| 2.0.0 | [SPARK-15292](https://issues.apache.org/jira/browse/SPARK-15292) | Improvement | ML 2.0 QA: Scala APIs audit for classification |
| 2.0.0 | [SPARK-15322](https://issues.apache.org/jira/browse/SPARK-15322) | Improvement | update deprecate accumulator usage into accumulatorV2 in mllib |
| 2.0.0 | [SPARK-15339](https://issues.apache.org/jira/browse/SPARK-15339) | Improvement | ML 2.0 QA: Scala APIs and code audit for regression |
| 2.0.0 | [SPARK-15346](https://issues.apache.org/jira/browse/SPARK-15346) | Improvement | Reduce duplicate computation in picking initial points in LocalKMeans |
| 2.0.0 | [SPARK-15361](https://issues.apache.org/jira/browse/SPARK-15361) | Improvement | ML 2.0 QA: Scala APIs audit for clustering |
| 2.0.0 | [SPARK-15362](https://issues.apache.org/jira/browse/SPARK-15362) | Improvement | Make spark.ml KMeansModel load backwards compatible |
| 2.0.0 | [SPARK-15364](https://issues.apache.org/jira/browse/SPARK-15364) | Improvement | Implement Python picklers for ml.Vector and ml.Matrix under spark.ml.python |
| 2.0.0 | [SPARK-15412](https://issues.apache.org/jira/browse/SPARK-15412) | Improvement | Improve linear & isotonic regression methods PyDocs |
| 2.0.0 | [SPARK-15414](https://issues.apache.org/jira/browse/SPARK-15414) | Improvement | Make the mllib,ml linalg type conversion APIs public |
| 2.0.0 | [SPARK-15442](https://issues.apache.org/jira/browse/SPARK-15442) | Improvement | PySpark QuantileDiscretizer missing "relativeError" param |
| 2.0.0 | [SPARK-15457](https://issues.apache.org/jira/browse/SPARK-15457) | Improvement | Eliminate MLlib 2.0 build warnings from deprecations |
| 2.0.0 | [SPARK-15484](https://issues.apache.org/jira/browse/SPARK-15484) | Improvement | Document Iteratively reweighted least squares (IRLS) in user guide |
| 2.0.0 | [SPARK-15501](https://issues.apache.org/jira/browse/SPARK-15501) | Improvement | ML 2.0 QA: Scala APIs audit for recommendation |
| 2.0.0 | [SPARK-15603](https://issues.apache.org/jira/browse/SPARK-15603) | Improvement | Replace SQLContext with SparkSession in ML/MLLib |
| 2.0.0 | [SPARK-15623](https://issues.apache.org/jira/browse/SPARK-15623) | Improvement | 2.0 python coverage ml.feature |
| 2.0.0 | [SPARK-15644](https://issues.apache.org/jira/browse/SPARK-15644) | Improvement | Replace SQLContext with SparkSession in MLlib |
| 2.0.0 | [SPARK-15721](https://issues.apache.org/jira/browse/SPARK-15721) | Improvement | Make DefaultParamsReadable,Writable public APIs |
| 2.0.0 | [SPARK-15738](https://issues.apache.org/jira/browse/SPARK-15738) | Improvement | PySpark ml.feature RFormula missing string representation displaying formula |
| 2.0.0 | [SPARK-15793](https://issues.apache.org/jira/browse/SPARK-15793) | Improvement | Word2vec in ML package should have maxSentenceLength method |
| 2.0.0 | [SPARK-15823](https://issues.apache.org/jira/browse/SPARK-15823) | Improvement | Add @property for 'accuracy' in MulticlassMetrics |
| 2.0.0 | [SPARK-15837](https://issues.apache.org/jira/browse/SPARK-15837) | Improvement | PySpark ML Word2Vec should support maxSentenceLength |
| 2.0.0 | [SPARK-15973](https://issues.apache.org/jira/browse/SPARK-15973) | Improvement | Fix GroupedData Documentation |
| 2.0.0 | [SPARK-16008](https://issues.apache.org/jira/browse/SPARK-16008) | Improvement | ML Logistic Regression aggregator serializes unnecessary data |
| 2.0.0 | [SPARK-16045](https://issues.apache.org/jira/browse/SPARK-16045) | Improvement | Spark 2.0 ML.feature: doc update for stopwords and binarizer |
| 2.0.0 | [SPARK-16074](https://issues.apache.org/jira/browse/SPARK-16074) | New Feature | Expose VectorUDT/MatrixUDT in a public API |
| 2.0.0 | [SPARK-16117](https://issues.apache.org/jira/browse/SPARK-16117) | Improvement | Hide LibSVMFileFormat in public API docs |
| 2.0.0 | [SPARK-16118](https://issues.apache.org/jira/browse/SPARK-16118) | New Feature | getDropLast is missing in OneHotEncoder |
| 2.0.0 | [SPARK-16130](https://issues.apache.org/jira/browse/SPARK-16130) | Improvement | model loading backward compatibility for ml.classfication.LogisticRegression |
| 2.0.0 | [SPARK-16133](https://issues.apache.org/jira/browse/SPARK-16133) | Improvement | model loading backward compatibility for ml.feature |
| 2.0.0 | [SPARK-16154](https://issues.apache.org/jira/browse/SPARK-16154) | Improvement | Update spark.ml and spark.mllib package docs |
| 2.0.0 | [SPARK-16177](https://issues.apache.org/jira/browse/SPARK-16177) | Improvement | model loading backward compatibility for ml.regression |
| 2.0.0 | [SPARK-16241](https://issues.apache.org/jira/browse/SPARK-16241) | Improvement | model loading backward compatibility for ml NaiveBayes |
| 2.0.0 | [SPARK-16245](https://issues.apache.org/jira/browse/SPARK-16245) | Improvement | model loading backward compatibility for ml.feature.PCA |
| 2.0.0 | [SPARK-16249](https://issues.apache.org/jira/browse/SPARK-16249) | Improvement | Change visibility of Object ml.clustering.LDA to public for loading |
| 2.0.0 | [SPARK-16470](https://issues.apache.org/jira/browse/SPARK-16470) | Improvement | ml.regression.LinearRegression training data do not check whether the result actually reach convergence |
| 2.0.0 | [SPARK-16500](https://issues.apache.org/jira/browse/SPARK-16500) | Improvement | Add LBFG training not convergence warning for all ML algorithm |
| 2.0.1 | [SPARK-10835](https://issues.apache.org/jira/browse/SPARK-10835) | Improvement | Word2Vec should accept non-null string array, in addition to existing null string array |
| 2.0.1 | [SPARK-16240](https://issues.apache.org/jira/browse/SPARK-16240) | Improvement | model loading backward compatibility for ml.clustering.LDA |
| 2.1.0 | [SPARK-3261](https://issues.apache.org/jira/browse/SPARK-3261) | Improvement | KMeans clusterer can return duplicate cluster centers |
| 2.1.0 | [SPARK-7159](https://issues.apache.org/jira/browse/SPARK-7159) | New Feature | Support multiclass logistic regression in spark.ml |
| 2.1.0 | [SPARK-10835](https://issues.apache.org/jira/browse/SPARK-10835) | Improvement | Word2Vec should accept non-null string array, in addition to existing null string array |
| 2.1.0 | [SPARK-11560](https://issues.apache.org/jira/browse/SPARK-11560) | Improvement | Optimize KMeans implementation / remove 'runs' from implementation |
| 2.1.0 | [SPARK-14077](https://issues.apache.org/jira/browse/SPARK-14077) | New Feature | Support weighted instances in naive Bayes |
| 2.1.0 | [SPARK-14610](https://issues.apache.org/jira/browse/SPARK-14610) | Improvement | Remove superfluous split from random forest findSplitsForContinousFeature |
| 2.1.0 | [SPARK-14634](https://issues.apache.org/jira/browse/SPARK-14634) | New Feature | Add BisectingKMeansSummary |
| 2.1.0 | [SPARK-15018](https://issues.apache.org/jira/browse/SPARK-15018) | Improvement | PySpark ML Pipeline raises unclear error when no stages set |
| 2.1.0 | [SPARK-15113](https://issues.apache.org/jira/browse/SPARK-15113) | Improvement | Add missing numFeatures & numClasses to wrapped JavaClassificationModel |
| 2.1.0 | [SPARK-15402](https://issues.apache.org/jira/browse/SPARK-15402) | Improvement | PySpark ml.evaluation should support save/load |
| 2.1.0 | [SPARK-15509](https://issues.apache.org/jira/browse/SPARK-15509) | Improvement | R MLlib algorithms should support input columns "features" and "label" |
| 2.1.0 | [SPARK-15581](https://issues.apache.org/jira/browse/SPARK-15581) | Umbrella | MLlib 2.1 Roadmap |
| 2.1.0 | [SPARK-15819](https://issues.apache.org/jira/browse/SPARK-15819) | Improvement | Add KMeanSummary in KMeans of PySpark |
| 2.1.0 | [SPARK-15944](https://issues.apache.org/jira/browse/SPARK-15944) | Umbrella | Make spark.ml package backward compatible with spark.mllib vectors |
| 2.1.0 | [SPARK-15957](https://issues.apache.org/jira/browse/SPARK-15957) | Improvement | RFormula supports forcing to index label |
| 2.1.0 | [SPARK-16000](https://issues.apache.org/jira/browse/SPARK-16000) | prose | ML persistence: backward-compatible model loading for Spark 1.x saved models |
| 2.1.0 | [SPARK-16240](https://issues.apache.org/jira/browse/SPARK-16240) | Improvement | model loading backward compatibility for ml.clustering.LDA |
| 2.1.0 | [SPARK-16653](https://issues.apache.org/jira/browse/SPARK-16653) | Improvement | Make convergence tolerance param in ANN default value consistent with other algorithm using LBFGS |
| 2.1.0 | [SPARK-16719](https://issues.apache.org/jira/browse/SPARK-16719) | Improvement | RandomForest: communicate fewer trees on each iteration |
| 2.1.0 | [SPARK-16933](https://issues.apache.org/jira/browse/SPARK-16933) | Improvement | AFTAggregator in AFTSurvivalRegression serializes unnecessary data |
| 2.1.0 | [SPARK-16934](https://issues.apache.org/jira/browse/SPARK-16934) | Improvement | Update LogisticCostAggregator serialization code to make it consistent with LinearRegression |
| 2.1.0 | [SPARK-17001](https://issues.apache.org/jira/browse/SPARK-17001) | Improvement | Enable standardScaler to standardize sparse vectors when withMean=True |
| 2.1.0 | [SPARK-17017](https://issues.apache.org/jira/browse/SPARK-17017) | New Feature | Add a chiSquare Selector based on False Positive Rate (FPR) test |
| 2.1.0 | [SPARK-17057](https://issues.apache.org/jira/browse/SPARK-17057) | Improvement | ProbabilisticClassifierModels' thresholds should have at most one 0 |
| 2.1.0 | [SPARK-17173](https://issues.apache.org/jira/browse/SPARK-17173) | Improvement | Refactor R mllib for easier ml implementations |
| 2.1.0 | [SPARK-17219](https://issues.apache.org/jira/browse/SPARK-17219) | Improvement | QuantileDiscretizer should handle NaN values gracefully |
| 2.1.0 | [SPARK-17281](https://issues.apache.org/jira/browse/SPARK-17281) | Improvement | Add treeAggregateDepth parameter for AFTSurvivalRegression |
| 2.1.0 | [SPARK-17311](https://issues.apache.org/jira/browse/SPARK-17311) | Improvement | Standardize Python-Java MLlib API to accept optional long seeds in all cases |
| 2.1.0 | [SPARK-17389](https://issues.apache.org/jira/browse/SPARK-17389) | Improvement | KMeans speedup with better choice of k-means\|\| init steps = 2 |
| 2.1.0 | [SPARK-17462](https://issues.apache.org/jira/browse/SPARK-17462) | Improvement | Check for places within MLlib which should use VersionUtils to parse Spark version strings |
| 2.1.0 | [SPARK-17507](https://issues.apache.org/jira/browse/SPARK-17507) | Improvement | check weight vector size in ANN |
| 2.1.0 | [SPARK-17595](https://issues.apache.org/jira/browse/SPARK-17595) | Improvement | Inefficient selection in Word2VecModel.findSynonyms |
| 2.1.0 | [SPARK-17704](https://issues.apache.org/jira/browse/SPARK-17704) | Improvement | ChiSqSelector performance improvement. |
| 2.1.0 | [SPARK-17744](https://issues.apache.org/jira/browse/SPARK-17744) | Improvement | Parity check between the ml and mllib test suites for NB |
| 2.1.0 | [SPARK-17748](https://issues.apache.org/jira/browse/SPARK-17748) | New Feature | One-pass algorithm for linear regression with L1 and elastic-net penalties |
| 2.1.0 | [SPARK-17835](https://issues.apache.org/jira/browse/SPARK-17835) | Improvement | Optimize NaiveBayes mllib wrapper to eliminate extra pass on data |
| 2.1.0 | [SPARK-18088](https://issues.apache.org/jira/browse/SPARK-18088) | Improvement | ChiSqSelector FPR PR cleanups |
| 2.1.0 | [SPARK-18177](https://issues.apache.org/jira/browse/SPARK-18177) | New Feature | Add missing 'subsamplingRate' of pyspark GBTClassifier |
| 2.1.0 | [SPARK-18282](https://issues.apache.org/jira/browse/SPARK-18282) | New Feature | Add model summaries for Python GMM and BisectingKMeans |
| 2.1.0 | [SPARK-18316](https://issues.apache.org/jira/browse/SPARK-18316) | Umbrella | Spark MLlib, GraphX 2.1 QA umbrella |
| 2.1.0 | [SPARK-18366](https://issues.apache.org/jira/browse/SPARK-18366) | New Feature | Add handleInvalid to Pyspark for QuantileDiscretizer and Bucketizer |
| 2.1.0 | [SPARK-18408](https://issues.apache.org/jira/browse/SPARK-18408) | Improvement | API Improvements for LSH |
| 2.1.0 | [SPARK-18427](https://issues.apache.org/jira/browse/SPARK-18427) | Improvement | Update docs of mllib.KMeans |
| 2.1.0 | [SPARK-18434](https://issues.apache.org/jira/browse/SPARK-18434) | Improvement | Add missing ParamValidations for ML algos |
| 2.1.0 | [SPARK-18438](https://issues.apache.org/jira/browse/SPARK-18438) | Improvement | spark.mlp should support RFormula |
| 2.1.0 | [SPARK-18446](https://issues.apache.org/jira/browse/SPARK-18446) | Improvement | make sure all ML algos have links to API docs |
| 2.1.0 | [SPARK-18456](https://issues.apache.org/jira/browse/SPARK-18456) | Improvement | Use matrix abstraction for LogisticRegression coefficients during training |
| 2.1.0 | [SPARK-18520](https://issues.apache.org/jira/browse/SPARK-18520) | Improvement | Add missing setXXXCol methods for BisectingKMeansModel and GaussianMixtureModel |
| 2.1.0 | [SPARK-18592](https://issues.apache.org/jira/browse/SPARK-18592) | Improvement | Move DT/RF/GBT Param setter methods to subclasses |
| 2.1.0 | [SPARK-18612](https://issues.apache.org/jira/browse/SPARK-18612) | Improvement | Leaked broadcasted variable in LBFGS |
| 2.1.0 | [SPARK-18625](https://issues.apache.org/jira/browse/SPARK-18625) | Improvement | OneVsRestModel should support setFeaturesCol and setPredictionCol |
| 2.1.0 | [SPARK-18686](https://issues.apache.org/jira/browse/SPARK-18686) | Improvement | Several cleanup and improvements for spark.logit |
| 2.2.0 | [SPARK-6227](https://issues.apache.org/jira/browse/SPARK-6227) | Improvement | PCA and SVD for PySpark |
| 2.2.0 | [SPARK-11569](https://issues.apache.org/jira/browse/SPARK-11569) | Improvement | StringIndexer transform fails when column contains nulls |
| 2.2.0 | [SPARK-11968](https://issues.apache.org/jira/browse/SPARK-11968) | Improvement | ALS recommend all methods spend most of time in GC |
| 2.2.0 | [SPARK-13568](https://issues.apache.org/jira/browse/SPARK-13568) | New Feature | Create feature transformer to impute missing values |
| 2.2.0 | [SPARK-14272](https://issues.apache.org/jira/browse/SPARK-14272) | Improvement | Evaluate GaussianMixtureModel with LogLikelihood |
| 2.2.0 | [SPARK-14503](https://issues.apache.org/jira/browse/SPARK-14503) | prose | FPGrowth frequent pattern mining and AssociationRules |
| 2.2.0 | [SPARK-14567](https://issues.apache.org/jira/browse/SPARK-14567) | Umbrella | Add instrumentation logs to MLlib training algorithms |
| 2.2.0 | [SPARK-14709](https://issues.apache.org/jira/browse/SPARK-14709) | New Feature | spark.ml API for linear SVM |
| 2.2.0 | [SPARK-14975](https://issues.apache.org/jira/browse/SPARK-14975) | New Feature | Predicted Probability per training instance for Gradient Boosted Trees |
| 2.2.0 | [SPARK-15040](https://issues.apache.org/jira/browse/SPARK-15040) | New Feature | PySpark impl for ml.feature.Imputer |
| 2.2.0 | [SPARK-17498](https://issues.apache.org/jira/browse/SPARK-17498) | Improvement | StringIndexer.setHandleInvalid should have another option 'new' |
| 2.2.0 | [SPARK-17629](https://issues.apache.org/jira/browse/SPARK-17629) | New Feature | Add local version of Word2Vec findSynonyms for spark.ml |
| 2.2.0 | [SPARK-17645](https://issues.apache.org/jira/browse/SPARK-17645) | New Feature | Add feature selector methods based on: False Discovery Rate (FDR) and Family Wise Error rate (FWE) |
| 2.2.0 | [SPARK-17747](https://issues.apache.org/jira/browse/SPARK-17747) | Improvement | WeightCol support non-double datatypes |
| 2.2.0 | [SPARK-17847](https://issues.apache.org/jira/browse/SPARK-17847) | Improvement | Reduce shuffled data size of GaussianMixture & copy the implementation from mllib to ml |
| 2.2.0 | [SPARK-18218](https://issues.apache.org/jira/browse/SPARK-18218) | Improvement | Optimize BlockMatrix multiplication, which may cause OOM and low parallelism usage problem in several cases |
| 2.2.0 | [SPARK-18239](https://issues.apache.org/jira/browse/SPARK-18239) | prose | Gradient Boosted Trees added to Python and R APIs |
| 2.2.0 | [SPARK-18356](https://issues.apache.org/jira/browse/SPARK-18356) | Improvement | KMeans should cache RDD before training |
| 2.2.0 | [SPARK-18613](https://issues.apache.org/jira/browse/SPARK-18613) | Improvement | spark.ml LDA classes should not expose spark.mllib in APIs |
| 2.2.0 | [SPARK-18698](https://issues.apache.org/jira/browse/SPARK-18698) | Improvement | public constructor with uid for IndexToString-class |
| 2.2.0 | [SPARK-18808](https://issues.apache.org/jira/browse/SPARK-18808) | Improvement | ml.KMeansModel.transform is very inefficient |
| 2.2.0 | [SPARK-18813](https://issues.apache.org/jira/browse/SPARK-18813) | Umbrella | MLlib 2.2 Roadmap |
| 2.2.0 | [SPARK-18901](https://issues.apache.org/jira/browse/SPARK-18901) | Improvement | Require in LR LogisticAggregator is redundant |
| 2.2.0 | [SPARK-18929](https://issues.apache.org/jira/browse/SPARK-18929) | New Feature | Add Tweedie distribution in GLM |
| 2.2.0 | [SPARK-19247](https://issues.apache.org/jira/browse/SPARK-19247) | Improvement | Improve ml word2vec save/load scalability |
| 2.2.0 | [SPARK-19384](https://issues.apache.org/jira/browse/SPARK-19384) | Improvement | forget unpersist input dataset in IsotonicRegression |
| 2.2.0 | [SPARK-19535](https://issues.apache.org/jira/browse/SPARK-19535) | New Feature | ALSModel recommendAll analogs |
| 2.2.0 | [SPARK-19635](https://issues.apache.org/jira/browse/SPARK-19635) | prose | ChiSquare test in DataFrame-based API |
| 2.2.0 | [SPARK-19636](https://issues.apache.org/jira/browse/SPARK-19636) | prose | Correlation in DataFrame-based API |
| 2.2.0 | [SPARK-19694](https://issues.apache.org/jira/browse/SPARK-19694) | Improvement | Add missing 'setTopicDistributionCol' for LDAModel |
| 2.2.0 | [SPARK-19704](https://issues.apache.org/jira/browse/SPARK-19704) | Improvement | AFTSurvivalRegression should support numeric censorCol |
| 2.2.0 | [SPARK-19733](https://issues.apache.org/jira/browse/SPARK-19733) | Improvement | ALS performs unnecessary casting on item and user ids |
| 2.2.0 | [SPARK-19746](https://issues.apache.org/jira/browse/SPARK-19746) | Improvement | LogisticAggregator is inefficient in indexing |
| 2.2.0 | [SPARK-19787](https://issues.apache.org/jira/browse/SPARK-19787) | Improvement | Different default regParam values in ALS |
| 2.2.0 | [SPARK-19899](https://issues.apache.org/jira/browse/SPARK-19899) | Improvement | FPGrowth input column naming |
| 2.2.0 | [SPARK-19922](https://issues.apache.org/jira/browse/SPARK-19922) | Improvement | faster Word2Vec findSynonyms |
| 2.2.0 | [SPARK-20011](https://issues.apache.org/jira/browse/SPARK-20011) | Improvement | inconsistent terminology in als api docs and tutorial |
| 2.2.0 | [SPARK-20039](https://issues.apache.org/jira/browse/SPARK-20039) | Improvement | Rename ml.stat.ChiSquare to ml.stat.ChiSquareTest |
| 2.2.0 | [SPARK-20040](https://issues.apache.org/jira/browse/SPARK-20040) | New Feature | Python API for ml.stat.ChiSquareTest |
| 2.2.0 | [SPARK-20047](https://issues.apache.org/jira/browse/SPARK-20047) | New Feature | Constrained Logistic Regression |
| 2.2.0 | [SPARK-20300](https://issues.apache.org/jira/browse/SPARK-20300) | New Feature | Python API for ALSModel.recommendForAllUsers,Items |
| 2.2.0 | [SPARK-20404](https://issues.apache.org/jira/browse/SPARK-20404) | Improvement | Regression with accumulator names when migrating from 1.6 to 2.x |
| 2.2.0 | [SPARK-20499](https://issues.apache.org/jira/browse/SPARK-20499) | Umbrella | Spark MLlib, GraphX 2.2 QA umbrella |
| 2.2.0 | [SPARK-20587](https://issues.apache.org/jira/browse/SPARK-20587) | Improvement | Improve performance of ML ALS recommendForAll |
| 2.2.0 | [SPARK-20669](https://issues.apache.org/jira/browse/SPARK-20669) | Improvement | LogisticRegression family should be case insensitive |
| 2.2.0 | [SPARK-20677](https://issues.apache.org/jira/browse/SPARK-20677) | Improvement | Clean up ALS recommend all improvement code. |
| 2.2.0 | [SPARK-20768](https://issues.apache.org/jira/browse/SPARK-20768) | Improvement | PySpark FPGrowth does not expose numPartitions (expert) param |
| 2.2.0 | [SPARK-20861](https://issues.apache.org/jira/browse/SPARK-20861) | Improvement | Pyspark CrossValidator & TrainValidationSplit should delegate parameter looping to estimators |
| 2.3.0 | [SPARK-3181](https://issues.apache.org/jira/browse/SPARK-3181) | prose | Robust linear regression with Huber loss |
| 2.3.0 | [SPARK-13030](https://issues.apache.org/jira/browse/SPARK-13030) | prose | OneHotEncoderEstimator multi-column support |
| 2.3.0 | [SPARK-13969](https://issues.apache.org/jira/browse/SPARK-13969) | prose | FeatureHasher transformer |
| 2.3.0 | [SPARK-14371](https://issues.apache.org/jira/browse/SPARK-14371) | prose | OnlineLDAOptimizer avoids collecting statistics to driver per mini-batch |
| 2.3.0 | [SPARK-14516](https://issues.apache.org/jira/browse/SPARK-14516) | prose | ClusteringEvaluator for tuning clustering algorithms |
| 2.3.0 | [SPARK-17139](https://issues.apache.org/jira/browse/SPARK-17139) | prose | Model summary for multinomial logistic regression |
| 2.3.0 | [SPARK-18710](https://issues.apache.org/jira/browse/SPARK-18710) | prose | Offset support added to GLM |
| 2.3.0 | [SPARK-19357](https://issues.apache.org/jira/browse/SPARK-19357) | prose | Parallelism Param for CrossValidator/TrainValidationSplit/OneVsRest |
| 2.3.0 | [SPARK-19634](https://issues.apache.org/jira/browse/SPARK-19634) | prose | DataFrame functions for descriptive summary statistics over vector columns |
| 2.3.0 | [SPARK-20199](https://issues.apache.org/jira/browse/SPARK-20199) | prose | featureSubsetStrategy Param added to GBTClassifier/GBTRegressor |
| 2.3.0 | [SPARK-20542](https://issues.apache.org/jira/browse/SPARK-20542) | prose | Bucketizer multi-column support |
| 2.3.0 | [SPARK-21087](https://issues.apache.org/jira/browse/SPARK-21087) | prose | CrossValidator/TrainValidationSplit can collect all fitted models |
| 2.3.0 | [SPARK-21633](https://issues.apache.org/jira/browse/SPARK-21633) | prose | Improved support for custom pipeline components in Python |
| 2.3.0 | [SPARK-21690](https://issues.apache.org/jira/browse/SPARK-21690) | prose | Imputer trains using a single pass over the data |
| 2.3.0 | [SPARK-22397](https://issues.apache.org/jira/browse/SPARK-22397) | prose | QuantileDiscretizer multi-column support |
| 2.3.0 | [SPARK-22707](https://issues.apache.org/jira/browse/SPARK-22707) | prose | Reduced memory consumption for CrossValidator |
| 2.3.0 | [SPARK-22949](https://issues.apache.org/jira/browse/SPARK-22949) | prose | Reduced memory consumption for TrainValidationSplit |
| 2.4.0 | [SPARK-7132](https://issues.apache.org/jira/browse/SPARK-7132) | prose | Fit with validation set added to spark.ml GBT |
| 2.4.0 | [SPARK-10697](https://issues.apache.org/jira/browse/SPARK-10697) | prose | Lift calculation in Association Rule mining |
| 2.4.0 | [SPARK-10884](https://issues.apache.org/jira/browse/SPARK-10884) | prose | Prediction on single instance for regression/classification models |
| 2.4.0 | [SPARK-11239](https://issues.apache.org/jira/browse/SPARK-11239) | prose | PMML export for ML linear regression |
| 2.4.0 | [SPARK-14682](https://issues.apache.org/jira/browse/SPARK-14682) | prose | evaluateEachIteration method for spark.ml GBTs |
| 2.4.0 | [SPARK-15064](https://issues.apache.org/jira/browse/SPARK-15064) | prose | Locale support in StopWordsRemover |
| 2.4.0 | [SPARK-15784](https://issues.apache.org/jira/browse/SPARK-15784) | prose | Power Iteration Clustering added to spark.ml |
| 2.4.0 | [SPARK-21741](https://issues.apache.org/jira/browse/SPARK-21741) | prose | Python API for DataFrame-based multivariate summarizer |
| 2.4.0 | [SPARK-21898](https://issues.apache.org/jira/browse/SPARK-21898) | prose | Feature parity for KolmogorovSmirnovTest in MLlib |
| 2.4.0 | [SPARK-22119](https://issues.apache.org/jira/browse/SPARK-22119) | prose | Cosine distance measure added to KMeans/BisectingKMeans/ClusteringEvaluator |
| 2.4.0 | [SPARK-23783](https://issues.apache.org/jira/browse/SPARK-23783) | prose | New generic export trait for ML pipelines |
| 3.0.0 | [SPARK-9478](https://issues.apache.org/jira/browse/SPARK-9478) | Improvement | Add sample weights to Random Forest |
| 3.0.0 | [SPARK-9612](https://issues.apache.org/jira/browse/SPARK-9612) | prose | ), GBTClassifier/Regressor |
| 3.0.0 | [SPARK-11215](https://issues.apache.org/jira/browse/SPARK-11215) | Improvement | Add multiple columns support to StringIndexer |
| 3.0.0 | [SPARK-16692](https://issues.apache.org/jira/browse/SPARK-16692) | Improvement | multilabel classification to DataFrame, ML |
| 3.0.0 | [SPARK-16838](https://issues.apache.org/jira/browse/SPARK-16838) | Improvement | Add PMML export for ML KMeans in PySpark |
| 3.0.0 | [SPARK-16872](https://issues.apache.org/jira/browse/SPARK-16872) | New Feature | Impl Gaussian Naive Bayes Classifier |
| 3.0.0 | [SPARK-18299](https://issues.apache.org/jira/browse/SPARK-18299) | Improvement | Allow more aggregations on KeyValueGroupedDataset |
| 3.0.0 | [SPARK-19368](https://issues.apache.org/jira/browse/SPARK-19368) | Improvement | Very bad performance in BlockMatrix.toIndexedRowMatrix() |
| 3.0.0 | [SPARK-19591](https://issues.apache.org/jira/browse/SPARK-19591) | New Feature | Add sample weights to decision trees |
| 3.0.0 | [SPARK-19714](https://issues.apache.org/jira/browse/SPARK-19714) | Improvement | Clarify Bucketizer handling of invalid input |
| 3.0.0 | [SPARK-19827](https://issues.apache.org/jira/browse/SPARK-19827) | prose | R API for PowerIterationClustering was added |
| 3.0.0 | [SPARK-20604](https://issues.apache.org/jira/browse/SPARK-20604) | Improvement | Allow Imputer to handle all numeric types |
| 3.0.0 | [SPARK-21481](https://issues.apache.org/jira/browse/SPARK-21481) | Improvement | Add indexOf method in ml.feature.HashingTF similar to mllib.feature.HashingTF |
| 3.0.0 | [SPARK-22796](https://issues.apache.org/jira/browse/SPARK-22796) | New Feature | Add multiple column support to PySpark QuantileDiscretizer |
| 3.0.0 | [SPARK-22797](https://issues.apache.org/jira/browse/SPARK-22797) | New Feature | Add multiple column support to PySpark Bucketizer |
| 3.0.0 | [SPARK-22798](https://issues.apache.org/jira/browse/SPARK-22798) | New Feature | Add multiple column support to PySpark StringIndexer |
| 3.0.0 | [SPARK-23265](https://issues.apache.org/jira/browse/SPARK-23265) | Improvement | Update multi-column error handling logic in QuantileDiscretizer |
| 3.0.0 | [SPARK-23578](https://issues.apache.org/jira/browse/SPARK-23578) | Improvement | Add multicolumn support for Binarizer |
| 3.0.0 | [SPARK-23674](https://issues.apache.org/jira/browse/SPARK-23674) | Improvement | Add Spark ML Listener for Tracking ML Pipeline Status |
| 3.0.0 | [SPARK-24101](https://issues.apache.org/jira/browse/SPARK-24101) | Improvement | MulticlassClassificationEvaluator should use sample weight data |
| 3.0.0 | [SPARK-24102](https://issues.apache.org/jira/browse/SPARK-24102) | Improvement | RegressionEvaluator should use sample weight data |
| 3.0.0 | [SPARK-24103](https://issues.apache.org/jira/browse/SPARK-24103) | Improvement | BinaryClassificationEvaluator should use sample weight data |
| 3.0.0 | [SPARK-24333](https://issues.apache.org/jira/browse/SPARK-24333) | New Feature | Add fit with validation set to spark.ml GBT: Python API |
| 3.0.0 | [SPARK-24489](https://issues.apache.org/jira/browse/SPARK-24489) | Improvement | No check for invalid input type of weight data in ml.PowerIterationClustering |
| 3.0.0 | [SPARK-25764](https://issues.apache.org/jira/browse/SPARK-25764) | Improvement | Avoid usage of deprecated methods in examples for BisectingKMeans |
| 3.0.0 | [SPARK-25765](https://issues.apache.org/jira/browse/SPARK-25765) | Improvement | Add trainingCost to BisectingKMeans summary |
| 3.0.0 | [SPARK-25790](https://issues.apache.org/jira/browse/SPARK-25790) | Improvement | PCA doesn't support more than 65535 column matrix |
| 3.0.0 | [SPARK-25868](https://issues.apache.org/jira/browse/SPARK-25868) | Improvement | One part of Spark MLlib Kmean Logic Performance problem |
| 3.0.0 | [SPARK-26006](https://issues.apache.org/jira/browse/SPARK-26006) | Improvement | mllib Prefixspan |
| 3.0.0 | [SPARK-26133](https://issues.apache.org/jira/browse/SPARK-26133) | Improvement | Remove deprecated OneHotEncoder and rename OneHotEncoderEstimator to OneHotEncoder |
| 3.0.0 | [SPARK-26153](https://issues.apache.org/jira/browse/SPARK-26153) | Improvement | GBT & RandomForest avoid unnecessary `first` job to compute `numFeatures` |
| 3.0.0 | [SPARK-26158](https://issues.apache.org/jira/browse/SPARK-26158) | Improvement | Enhance the accuracy of covariance in RowMatrix for DenseVector |
| 3.0.0 | [SPARK-26881](https://issues.apache.org/jira/browse/SPARK-26881) | Improvement | Scaling issue with Gramian computation for RowMatrix: too many results sent to driver |
| 3.0.0 | [SPARK-26970](https://issues.apache.org/jira/browse/SPARK-26970) | Improvement | Can't load PipelineModel that was created in Scala with Python due to missing Interaction transformer |
| 3.0.0 | [SPARK-26981](https://issues.apache.org/jira/browse/SPARK-26981) | New Feature | Add 'Recall_at_k' metric to RankingMetrics |
| 3.0.0 | [SPARK-27007](https://issues.apache.org/jira/browse/SPARK-27007) | Improvement | add rawPrediction to OneVsRest in PySpark |
| 3.0.0 | [SPARK-27410](https://issues.apache.org/jira/browse/SPARK-27410) | Improvement | Remove deprecated/no-op mllib.Kmeans get/setRuns methods |
| 3.0.0 | [SPARK-27540](https://issues.apache.org/jira/browse/SPARK-27540) | New Feature | Add 'meanAveragePrecision_at_k' metric to RankingMetrics |
| 3.0.0 | [SPARK-27847](https://issues.apache.org/jira/browse/SPARK-27847) | Improvement | One-Pass MultilabelMetrics & MulticlassMetrics |
| 3.0.0 | [SPARK-28044](https://issues.apache.org/jira/browse/SPARK-28044) | Improvement | MulticlassClassificationEvaluator support more metrics |
| 3.0.0 | [SPARK-28112](https://issues.apache.org/jira/browse/SPARK-28112) | Improvement | Fix Kryo exception perf. bottleneck in tests due to absence of ML/MLlib classes |
| 3.0.0 | [SPARK-28117](https://issues.apache.org/jira/browse/SPARK-28117) | Improvement | LDA and BisectingKMeans cache the input dataset if necessary |
| 3.0.0 | [SPARK-28140](https://issues.apache.org/jira/browse/SPARK-28140) | Improvement | Pyspark API to create spark.mllib RowMatrix from DataFrame |
| 3.0.0 | [SPARK-28170](https://issues.apache.org/jira/browse/SPARK-28170) | Improvement | DenseVector .toArray() and .values documentation do not specify they are aliases |
| 3.0.0 | [SPARK-28243](https://issues.apache.org/jira/browse/SPARK-28243) | Improvement | remove setFeatureSubsetStrategy and setSubsamplingRate from Python TreeEnsembleParams |
| 3.0.0 | [SPARK-28399](https://issues.apache.org/jira/browse/SPARK-28399) | New Feature | Impl RobustScaler |
| 3.0.0 | [SPARK-28421](https://issues.apache.org/jira/browse/SPARK-28421) | Improvement | SparseVector.apply performance optimization |
| 3.0.0 | [SPARK-28434](https://issues.apache.org/jira/browse/SPARK-28434) | Improvement | Decision Tree model isn't equal after save and load |
| 3.0.0 | [SPARK-28499](https://issues.apache.org/jira/browse/SPARK-28499) | Improvement | Optimize MinMaxScaler |
| 3.0.0 | [SPARK-28514](https://issues.apache.org/jira/browse/SPARK-28514) | Improvement | Remove the redundant transformImpl method in RF & GBT |
| 3.0.0 | [SPARK-28579](https://issues.apache.org/jira/browse/SPARK-28579) | Improvement | MaxAbsScaler avoids conversion to breeze.vector |
| 3.0.0 | [SPARK-28722](https://issues.apache.org/jira/browse/SPARK-28722) | Improvement | Change sequential label sorting in StringIndexer fit to parallel |
| 3.0.0 | [SPARK-28866](https://issues.apache.org/jira/browse/SPARK-28866) | Improvement | Persist item factors RDD when checkpointing in ALS |
| 3.0.0 | [SPARK-28927](https://issues.apache.org/jira/browse/SPARK-28927) | Improvement | Improve error for ArrayIndexOutOfBoundsException and Not-stable AUC metrics in ALS for datasets with 12 billion instances |
| 3.0.0 | [SPARK-28933](https://issues.apache.org/jira/browse/SPARK-28933) | Improvement | Reduce unnecessary shuffle in ALS when initializing factors |
| 3.0.0 | [SPARK-28958](https://issues.apache.org/jira/browse/SPARK-28958) | prose | ML function parity between Scala and Python |
| 3.0.0 | [SPARK-29093](https://issues.apache.org/jira/browse/SPARK-29093) | prose | , value) instead. See |
| 3.0.0 | [SPARK-29258](https://issues.apache.org/jira/browse/SPARK-29258) | Improvement | parity between ml.evaluator and mllib.metrics |
| 3.0.0 | [SPARK-29363](https://issues.apache.org/jira/browse/SPARK-29363) | Improvement | o.a.s.ml.regression.Regressor should be public |
| 3.0.0 | [SPARK-29380](https://issues.apache.org/jira/browse/SPARK-29380) | Improvement | RFormula avoid repeated 'first' jobs to get vector size |
| 3.0.0 | [SPARK-29427](https://issues.apache.org/jira/browse/SPARK-29427) | New Feature | Add API to convert RelationalGroupedDataset to KeyValueGroupedDataset |
| 3.0.0 | [SPARK-29464](https://issues.apache.org/jira/browse/SPARK-29464) | Improvement | PySpark ML should expose Params.clear() to unset a user supplied Param |
| 3.0.0 | [SPARK-29489](https://issues.apache.org/jira/browse/SPARK-29489) | New Feature | ml.evaluation support log-loss |
| 3.0.0 | [SPARK-29565](https://issues.apache.org/jira/browse/SPARK-29565) | New Feature | OneHotEncoder should support single-column input/ouput |
| 3.0.0 | [SPARK-29566](https://issues.apache.org/jira/browse/SPARK-29566) | New Feature | Imputer should support single-column input/ouput |
| 3.0.0 | [SPARK-29656](https://issues.apache.org/jira/browse/SPARK-29656) | Improvement | ML algs expose aggregationDepth |
| 3.0.0 | [SPARK-29751](https://issues.apache.org/jira/browse/SPARK-29751) | Improvement | Scalers use Summarizer instead of MultivariateOnlineSummarizer |
| 3.0.0 | [SPARK-29754](https://issues.apache.org/jira/browse/SPARK-29754) | Improvement | LoR/AFT/LiR/SVC use Summarizer instead of MultivariateOnlineSummarizer |
| 3.0.0 | [SPARK-29756](https://issues.apache.org/jira/browse/SPARK-29756) | Improvement | CountVectorizer forget to unpersist intermediate rdd |
| 3.0.0 | [SPARK-29801](https://issues.apache.org/jira/browse/SPARK-29801) | Improvement | ML models unify toString method |
| 3.0.0 | [SPARK-29808](https://issues.apache.org/jira/browse/SPARK-29808) | Improvement | StopWordsRemover should support multi-cols |
| 3.0.0 | [SPARK-29823](https://issues.apache.org/jira/browse/SPARK-29823) | Improvement | Improper persist strategy in mllib.clustering.KMeans.run() |
| 3.0.0 | [SPARK-29844](https://issues.apache.org/jira/browse/SPARK-29844) | Improvement | Improper unpersist strategy in ml.recommendation.ASL.train |
| 3.0.0 | [SPARK-29867](https://issues.apache.org/jira/browse/SPARK-29867) | Improvement | add __repr__ in Python ML Models |
| 3.0.0 | [SPARK-29942](https://issues.apache.org/jira/browse/SPARK-29942) | Improvement | Impl Complement Naive Bayes Classifier |
| 3.0.0 | [SPARK-29960](https://issues.apache.org/jira/browse/SPARK-29960) | Improvement | MulticlassClassificationEvaluator support hammingLoss |
| 3.0.0 | [SPARK-29967](https://issues.apache.org/jira/browse/SPARK-29967) | Improvement | KMeans support instance weighting |
| 3.0.0 | [SPARK-30044](https://issues.apache.org/jira/browse/SPARK-30044) | Improvement | MNB/CNB/BNB use empty matrix instead of null |
| 3.0.0 | [SPARK-30109](https://issues.apache.org/jira/browse/SPARK-30109) | Improvement | PCA use BLAS.gemv with sparse vector |
| 3.0.0 | [SPARK-30124](https://issues.apache.org/jira/browse/SPARK-30124) | Improvement | unnecessary persist in PythonMLLibAPI.scala |
| 3.0.0 | [SPARK-30146](https://issues.apache.org/jira/browse/SPARK-30146) | Improvement | add setWeightCol to GBTs in PySpark |
| 3.0.0 | [SPARK-30154](https://issues.apache.org/jira/browse/SPARK-30154) | New Feature | PySpark UDF to convert MLlib vectors to dense arrays |
| 3.0.0 | [SPARK-30178](https://issues.apache.org/jira/browse/SPARK-30178) | Improvement | RobustScaler support bigger numFeatures |
| 3.0.0 | [SPARK-30247](https://issues.apache.org/jira/browse/SPARK-30247) | Improvement | GaussianMixtureModel in py side should expose gaussian |
| 3.0.0 | [SPARK-30347](https://issues.apache.org/jira/browse/SPARK-30347) | Improvement | LibSVMDataSource attach AttributeGroup |
| 3.0.0 | [SPARK-30351](https://issues.apache.org/jira/browse/SPARK-30351) | Improvement | BisectingKMeans support instance weighting |
| 3.0.0 | [SPARK-30354](https://issues.apache.org/jira/browse/SPARK-30354) | Improvement | GBT reuse DecisionTreeMetadata among iterations |
| 3.0.0 | [SPARK-30358](https://issues.apache.org/jira/browse/SPARK-30358) | Improvement | ML expose predictRaw and predictProbability |
| 3.0.0 | [SPARK-30377](https://issues.apache.org/jira/browse/SPARK-30377) | Improvement | Make Regressors extend abstract class Regressor |
| 3.0.0 | [SPARK-30378](https://issues.apache.org/jira/browse/SPARK-30378) | Improvement | FM support getter of training params |
| 3.0.0 | [SPARK-30380](https://issues.apache.org/jira/browse/SPARK-30380) | Improvement | Refactor RandomForest.findSplits |
| 3.0.0 | [SPARK-30381](https://issues.apache.org/jira/browse/SPARK-30381) | Improvement | GBT reuse treePoints for all trees |
| 3.0.0 | [SPARK-30390](https://issues.apache.org/jira/browse/SPARK-30390) | Improvement | Avoid double caching in mllib.KMeans#runWithWeights. |
| 3.0.0 | [SPARK-30398](https://issues.apache.org/jira/browse/SPARK-30398) | Improvement | PCA/RegressionMetrics/RowMatrix avoid unnecessary computation |
| 3.0.0 | [SPARK-30418](https://issues.apache.org/jira/browse/SPARK-30418) | Improvement | make FM call super class method extractLabeledPoints |
| 3.0.0 | [SPARK-30452](https://issues.apache.org/jira/browse/SPARK-30452) | Improvement | Add predict and numFeatures in Python IsotonicRegressionModel |
| 3.0.0 | [SPARK-30457](https://issues.apache.org/jira/browse/SPARK-30457) | Improvement | Use PeriodicRDDCheckpointer instead of NodeIdCache |
| 3.0.0 | [SPARK-30491](https://issues.apache.org/jira/browse/SPARK-30491) | Improvement | Enable dependency audit files to tell dependency classifier |
| 3.0.0 | [SPARK-30502](https://issues.apache.org/jira/browse/SPARK-30502) | Improvement | PeriodicRDDCheckpointer supports storageLevel |
| 3.0.0 | [SPARK-30630](https://issues.apache.org/jira/browse/SPARK-30630) | Improvement | Deprecate numTrees in GBT at 2.4.5 and remove it at 3.0.0 |
| 3.0.0 | [SPARK-30700](https://issues.apache.org/jira/browse/SPARK-30700) | Improvement | NaiveBayesModel predict optimization |
| 3.0.0 | [SPARK-30995](https://issues.apache.org/jira/browse/SPARK-30995) | Improvement | Latex doesn't work correctly in FMClassifier/FMRegressor Scala doc |
| 3.0.0 | [SPARK-31606](https://issues.apache.org/jira/browse/SPARK-31606) | Improvement | reduce the perf regression of vectorized parquet reader caused by datetime rebase |
| 3.0.0 | [SPARK-31610](https://issues.apache.org/jira/browse/SPARK-31610) | Improvement | Expose hashFuncVersion property in HashingTF |
| 3.1.1 | [SPARK-19939](https://issues.apache.org/jira/browse/SPARK-19939) | prose | Add support for association rules in ML |
| 3.1.1 | [SPARK-20249](https://issues.apache.org/jira/browse/SPARK-20249) | prose | Add training summary for LinearSVCModel |
| 3.1.1 | [SPARK-23631](https://issues.apache.org/jira/browse/SPARK-23631) | prose | Add summary to RandomForestClassificationModel |
| 3.1.1 | [SPARK-30642](https://issues.apache.org/jira/browse/SPARK-30642) | prose | LinearSVC blockify input vectors |
| 3.1.1 | [SPARK-30659](https://issues.apache.org/jira/browse/SPARK-30659) | prose | LogisticRegression blockify input vectors |
| 3.1.1 | [SPARK-30660](https://issues.apache.org/jira/browse/SPARK-30660) | prose | LinearRegression blockify input vectors |
| 3.1.1 | [SPARK-30818](https://issues.apache.org/jira/browse/SPARK-30818) | prose | Add SparkR LinearRegression wrapper |
| 3.1.1 | [SPARK-30819](https://issues.apache.org/jira/browse/SPARK-30819) | prose | Add FMRegressor wrapper to SparkR |
| 3.1.1 | [SPARK-30820](https://issues.apache.org/jira/browse/SPARK-30820) | prose | Add FMClassifier to SparkR |
| 3.1.1 | [SPARK-31007](https://issues.apache.org/jira/browse/SPARK-31007) | prose | KMeans optimization based on triangle-inequality |
| 3.1.1 | [SPARK-31032](https://issues.apache.org/jira/browse/SPARK-31032) | prose | GMM compute summary and update distributions in one job |
| 3.1.1 | [SPARK-31077](https://issues.apache.org/jira/browse/SPARK-31077) | prose | Remove ChiSqSelector dependency on mllib.ChiSqSelectorModel |
| 3.1.1 | [SPARK-31301](https://issues.apache.org/jira/browse/SPARK-31301) | prose | Flatten the result dataframe of tests in testChiSquare |
| 3.1.1 | [SPARK-31436](https://issues.apache.org/jira/browse/SPARK-31436) | prose | MinHash keyDistance optimization |
| 3.1.1 | [SPARK-31656](https://issues.apache.org/jira/browse/SPARK-31656) | prose | AFT blockify input vectors |
| 3.1.1 | [SPARK-31734](https://issues.apache.org/jira/browse/SPARK-31734) | prose | Add weight support in ClusteringEvaluator |
| 3.1.1 | [SPARK-31768](https://issues.apache.org/jira/browse/SPARK-31768) | prose | Add getMetrics in Evaluators |
| 3.1.1 | [SPARK-31777](https://issues.apache.org/jira/browse/SPARK-31777) | prose | Add user-specified fold column to CrossValidator |
| 3.1.1 | [SPARK-31925](https://issues.apache.org/jira/browse/SPARK-31925) | prose | Summary.totalIterations greater than maxIters |
| 3.1.1 | [SPARK-31944](https://issues.apache.org/jira/browse/SPARK-31944) | prose | Add instance weight support in LinearRegressionSummary |
| 3.1.1 | [SPARK-32140](https://issues.apache.org/jira/browse/SPARK-32140) | prose | Add training summary to FMClassificationModel |
| 3.1.1 | [SPARK-32298](https://issues.apache.org/jira/browse/SPARK-32298) | prose | tree models prediction optimization |
| 3.1.1 | [SPARK-32310](https://issues.apache.org/jira/browse/SPARK-32310) | prose | ML params default value parity in feature and tuning |
| 3.1.1 | [SPARK-32449](https://issues.apache.org/jira/browse/SPARK-32449) | prose | Add summary to MultilayerPerceptronClassificationModel |
| 3.1.1 | [SPARK-32907](https://issues.apache.org/jira/browse/SPARK-32907) | prose | adaptively blockify instances - LinearSVC |
| 3.1.1 | [SPARK-32974](https://issues.apache.org/jira/browse/SPARK-32974) | prose | FeatureHasher transform optimization |
| 3.1.1 | [SPARK-33040](https://issues.apache.org/jira/browse/SPARK-33040) | prose | Add SparkR wrapper for vector_to_array |
| 3.1.1 | [SPARK-33111](https://issues.apache.org/jira/browse/SPARK-33111) | prose | aft transform optimization |
| 3.1.1 | [SPARK-33520](https://issues.apache.org/jira/browse/SPARK-33520) | prose | make CrossValidator/TrainValidateSplit/OneVsRest Reader/Writer support Python backend estimator/evaluator |
| 3.1.1 | [SPARK-33556](https://issues.apache.org/jira/browse/SPARK-33556) | prose | Add array_to_vector function for dataframe column |
| 3.1.1 | [SPARK-34080](https://issues.apache.org/jira/browse/SPARK-34080) | New Feature | Add UnivariateFeatureSelector to deprecate existing selectors |
| 3.2.0 | [SPARK-33518](https://issues.apache.org/jira/browse/SPARK-33518) | Improvement | Improve performance of ML ALS recommendForAll by GEMV |
| 3.2.0 | [SPARK-33609](https://issues.apache.org/jira/browse/SPARK-33609) | Improvement | word2vec reduce broadcast size |
| 3.2.0 | [SPARK-33882](https://issues.apache.org/jira/browse/SPARK-33882) | prose | Add a vectorized BLAS implementation |
| 3.2.0 | [SPARK-34045](https://issues.apache.org/jira/browse/SPARK-34045) | Improvement | OneVsRestModel.transform should not call setter of submodels |
| 3.2.0 | [SPARK-34047](https://issues.apache.org/jira/browse/SPARK-34047) | Improvement | save tree models in single partition |
| 3.2.0 | [SPARK-34080](https://issues.apache.org/jira/browse/SPARK-34080) | New Feature | Add UnivariateFeatureSelector to deprecate existing selectors |
| 3.2.0 | [SPARK-34189](https://issues.apache.org/jira/browse/SPARK-34189) | Improvement | w2v findSynonyms optimization |
| 3.2.0 | [SPARK-34256](https://issues.apache.org/jira/browse/SPARK-34256) | Improvement | VectorSlicer refine numFeatures checking and toString method |
| 3.2.0 | [SPARK-34291](https://issues.apache.org/jira/browse/SPARK-34291) | Improvement | LSH hashDistance optimization |
| 3.2.0 | [SPARK-34797](https://issues.apache.org/jira/browse/SPARK-34797) | prose | Refactor Logistic Aggregator - support virtual centering |
| 3.2.0 | [SPARK-35150](https://issues.apache.org/jira/browse/SPARK-35150) | Improvement | Accelerate fallback BLAS with dev.ludovic.netlib |
| 3.2.0 | [SPARK-35306](https://issues.apache.org/jira/browse/SPARK-35306) | Improvement | Add benchmark results for BLASBenchmark created by Github Actions machines |
| 3.2.0 | [SPARK-35707](https://issues.apache.org/jira/browse/SPARK-35707) | prose | optimize sparse GEMM by skipping bound checking |
| 3.2.0 | [SPARK-36578](https://issues.apache.org/jira/browse/SPARK-36578) | Improvement | Minor UnivariateFeatureSelector API doc improvement |
| 3.3.0 | [SPARK-35310](https://issues.apache.org/jira/browse/SPARK-35310) | prose | Update to breeze 1.2 |
| 3.3.0 | [SPARK-35848](https://issues.apache.org/jira/browse/SPARK-35848) | prose | Optimize some treeAggregates in MLlib by delaying allocations |
| 3.3.0 | [SPARK-36425](https://issues.apache.org/jira/browse/SPARK-36425) | prose | Support CrossValidatorModel get standard deviation of metrics for each paramMap |
| 3.3.0 | [SPARK-36481](https://issues.apache.org/jira/browse/SPARK-36481) | prose | Expose LogisticRegression.setInitialModel, like KMeans et al do |
| 3.3.0 | [SPARK-37118](https://issues.apache.org/jira/browse/SPARK-37118) | prose | Add distanceMeasure param to trainKMeansModel |
| 3.3.0 | [SPARK-37419](https://issues.apache.org/jira/browse/SPARK-37419) | prose | Rewrite _shared_params_code_gen.py to inline type hints for ml/param/shared.py |
| 3.4.0 | [SPARK-30661](https://issues.apache.org/jira/browse/SPARK-30661) | prose | KMeans blockify input vectors |
| 3.4.0 | [SPARK-38584](https://issues.apache.org/jira/browse/SPARK-38584) | prose | Unify the data validation |
| 3.4.0 | [SPARK-39446](https://issues.apache.org/jira/browse/SPARK-39446) | prose | Add relevance score for nDCG evaluation |
| 3.4.0 | [SPARK-40476](https://issues.apache.org/jira/browse/SPARK-40476) | prose | Reduce the shuffle size of ALS |
| 3.5.0 | [SPARK-42526](https://issues.apache.org/jira/browse/SPARK-42526) | prose | Add Classifier.getNumClasses back |
| 3.5.0 | [SPARK-42993](https://issues.apache.org/jira/browse/SPARK-42993) | prose | Make PyTorch Distributor compatible with Spark Connect |
| 3.5.0 | [SPARK-43516](https://issues.apache.org/jira/browse/SPARK-43516) | prose | Base interfaces of sparkML for spark3.5: estimator/transformer/model/evaluator |
| 3.5.0 | [SPARK-43783](https://issues.apache.org/jira/browse/SPARK-43783) | prose | Make MLv2 (ML on spark connect) supports pandas >= 2.0 |
| 3.5.0 | [SPARK-43981](https://issues.apache.org/jira/browse/SPARK-43981) | prose | Basic saving / loading implementation for ML on spark connect |
| 3.5.0 | [SPARK-43982](https://issues.apache.org/jira/browse/SPARK-43982) | prose | Implement pipeline estimator for ML on spark connect |
| 3.5.0 | [SPARK-43983](https://issues.apache.org/jira/browse/SPARK-43983) | prose | Implement cross validator estimator |
| 3.5.0 | [SPARK-44250](https://issues.apache.org/jira/browse/SPARK-44250) | prose | Implement classification evaluator |
| 4.0.0 | [SPARK-37178](https://issues.apache.org/jira/browse/SPARK-37178) | prose | Add Target Encoding to ml.feature |
| 4.0.0 | [SPARK-45547](https://issues.apache.org/jira/browse/SPARK-45547) | prose | Validate Vectors with built-in function |
| 4.0.0 | [SPARK-45757](https://issues.apache.org/jira/browse/SPARK-45757) | prose | Avoid re-computation of NNZ in Binarizer |
| 4.0.0 | [SPARK-48463](https://issues.apache.org/jira/browse/SPARK-48463) | prose | Make various ML transformers support nested input columns |
| 4.1.0 | [SPARK-51236](https://issues.apache.org/jira/browse/SPARK-51236) | prose | ML Connect improvements |
| 4.1.1 | [SPARK-54689](https://issues.apache.org/jira/browse/SPARK-54689) | Improvement | Make org.apache.spark.sql.pipelines internal package and make EstimatorUtils private |
<!-- AUTO:timeline END -->
