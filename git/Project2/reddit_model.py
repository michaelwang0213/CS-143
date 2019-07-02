from __future__ import print_function
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

import os
import cleantext as cleantext
from pyspark.sql.types import ArrayType
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import udf
from pyspark.ml.feature import CountVectorizer

from pyspark.ml.classification import LogisticRegression
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import BinaryClassificationEvaluator

from pyspark.ml.tuning import CrossValidatorModel
from pyspark.sql.functions import col
from pyspark.sql.types import BooleanType
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.functions import from_unixtime
from pyspark.sql.types import DateType


states = ['Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut', 'Delaware', 'District of Columbia', 'Florida', 'Georgia', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana', 'Maine', 'Maryland', 'Massachusetts', 'Michigan', 'Minnesota', 'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada', 'New Hampshire', 'New Jersey', 'New Mexico', 'New York', 'North Carolina', 'North Dakota', 'Ohio', 'Oklahoma', 'Oregon', 'Pennsylvania', 'Rhode Island', 'South Carolina', 'South Dakota', 'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia', 'Washington', 'West Virginia', 'Wisconsin', 'Wyoming']

def u_sanitize(text):
	return cleantext.sanitize(text)
#spark.udf.register("sanitizeWithPython", u_sanitize, ArrayType(StringType()))

def u_one(val):
	value = int(val)
	if value == 1:
		return 1
	else:
		return 0

def u_neg(val):
	value = int(val)
	if value == -1:
		return 1
	else:
		return 0

def u_clean_link_id(link):
	return(link[3:])

# due to my sanitize implementation, all /s and &gt; are removed, so this function doesn't do anything
def u_filter_comments(body):
	if body[0] == '&gt;':
		return False
	else:
		for token in body:
			if token == '\\s':
				return False
		return True

def u_bias_positive(values):
	if values[1] > 0.2:
		return 1
	else:
		return 0

def u_bias_negative(values):
	if values[1] > 0.25:
		return 1
	else:
		return 0


def main(context):
	"""Main function takes a Spark SQL context."""
	# YOUR CODE HERE
	# YOU MAY ADD OTHER FUNCTIONS AS NEEDED

	# 1
	parquetExists = os.path.isdir('comments.parquet')
	
	if not parquetExists:
		comments = context.read.json("comments-minimal.json.bz2")
		comments.write.parquet("comments.parquet")

		submissions = context.read.json("submissions.json.bz2")
		submissions.write.parquet("submissions.parquet")
		
		labeled_data = context.read.csv("labeled_data.csv", header=True)
		labeled_data.write.parquet("labeled_data.parquet")
	else:
		comments = context.read.parquet("comments.parquet")
		submissions = context.read.parquet("submissions.parquet")
		labeled_data = context.read.parquet("labeled_data.parquet")

	labeled_comments_parquetExists = os.path.isdir('labeled_comments.parquet')
	
	# 2
	if not labeled_comments_parquetExists:
		labeled_comments = labeled_data.join(comments, labeled_data.Input_id == comments.id)
		labeled_comments.write.parquet("labeled_comments.parquet")
	else:
		labeled_comments = context.read.parquet("labeled_comments.parquet")
	
	# 4, 5
	typed_sanitize = udf(u_sanitize, ArrayType(StringType()))

	labeled_sanitized = labeled_comments.select('Input_id', 'labeldjt', typed_sanitize('body').alias("sanitized"))

	# 6B
	typed_one = udf(u_one, IntegerType())
	typed_neg = udf(u_neg, IntegerType())
	labeled_sanitized = labeled_sanitized.select('Input_id',  'labeldjt', typed_one('labeldjt').alias('positive'), typed_neg('labeldjt').alias('negative'), 'sanitized')

	# 6A, taken from https://spark.apache.org/docs/latest/ml-features.html#countvectorizer
	cv = CountVectorizer(inputCol="sanitized", outputCol="counts", minDF=10.0, binary=True)
	model = cv.fit(labeled_sanitized)
	labeled_sanitized = model.transform(labeled_sanitized)

	#7
	models_exist = os.path.isdir('project2')
	
	if not models_exist:
		# Initialize two logistic regression models.
		# Replace labelCol with the column containing the label, and featuresCol with the column containing the features.
		poslr = LogisticRegression(labelCol="positive", featuresCol="counts", maxIter=10)
		neglr = LogisticRegression(labelCol="negative", featuresCol="counts", maxIter=10)
		# This is a binary classifier so we need an evaluator that knows how to deal with binary classifiers.
		posEvaluator = BinaryClassificationEvaluator(labelCol="positive")
		negEvaluator = BinaryClassificationEvaluator(labelCol="negative")
		# There are a few parameters associated with logistic regression. We do not know what they are a priori.
		# We do a grid search to find the best parameters. We can replace [1.0] with a list of values to try.
		# We will assume the parameter is 1.0. Grid search takes forever.
		posParamGrid = ParamGridBuilder().addGrid(poslr.regParam, [1.0]).build()
		negParamGrid = ParamGridBuilder().addGrid(neglr.regParam, [1.0]).build()
		# We initialize a 5 fold cross-validation pipeline.
		posCrossval = CrossValidator(
		    estimator=poslr,
		    evaluator=posEvaluator,
		    estimatorParamMaps=posParamGrid,
		    numFolds=5)
		negCrossval = CrossValidator(
		    estimator=neglr,
		    evaluator=negEvaluator,
		    estimatorParamMaps=negParamGrid,
		    numFolds=5)
		# Although crossvalidation creates its own train/test sets for
		# tuning, we still need a labeled test set, because it is not
		# accessible from the crossvalidator (argh!)
		# Split the data 50/50
		posTrain, posTest = labeled_sanitized.randomSplit([0.5, 0.5])
		negTrain, negTest = labeled_sanitized.randomSplit([0.5, 0.5])
		# Train the models
		print("Training positive classifier...")
		posModel = posCrossval.fit(posTrain)
		print("Training negative classifier...")
		negModel = negCrossval.fit(negTrain)

		# Once we train the models, we don't want to do it again. We can save the models and load them again later.
		posModel.save("project2/pos.model")
		negModel.save("project2/neg.model")
	else:
		posModel = CrossValidatorModel.load("project2/pos.model")
		negModel = CrossValidatorModel.load("project2/neg.model")

	#8
	negResult_exists = os.path.isdir('negResult.parquet')
	
	if not negResult_exists:
		clean_link_id = udf(u_clean_link_id)
		comments1 = comments.sample(False, 0.02, seed = 0)

		comments_8 = comments1.select('id', 'author_flair_text', 'created_utc', typed_sanitize('body').alias('sanitized'), clean_link_id('link_id').alias('link_id'))
		
		submissions_lite = submissions.select(col('id').alias('link_id'), 'title')

		comments_8 = comments_8.join(submissions_lite, comments_8.link_id == submissions_lite.link_id).select('id', 'sanitized', 'author_flair_text', 'created_utc', 'title')
		

		#9
		filter_comments = udf(u_filter_comments, BooleanType())
		# Doesn't do anything due to implementation of sanitize
		# comments_9 = comments_8.where(filter_comments(comments_8.sanitized))
		comments_9 = comments_8
		comments_9 = model.transform(comments_9)

		bias_positive = udf(u_bias_positive, IntegerType())
		posResult = posModel.transform(comments_9).select('id', col('author_flair_text').alias('flair'), 'created_utc', 'title', 'counts', bias_positive('probability').alias('pos%'))

		bias_negative = udf(u_bias_negative, IntegerType())
		negResult = negModel.transform(posResult).select('id', 'flair', 'created_utc', 'title', 'counts', 'pos%', bias_negative('probability').alias('neg%'))
	
		negResult.write.parquet("negResult.parquet")
	else:
		negResult = context.read.parquet("negResult.parquet")

	#10

	num_rows = negResult.count()
	pos_result_all_int = negResult.where(negResult['pos%'] == 1).count()
	pos_result_all = spark.createDataFrame([pos_result_all_int/num_rows], DoubleType()).toDF("pos%")
	neg_result_all_int = negResult.where(negResult['neg%'] == 1).count()
	neg_result_all = spark.createDataFrame([neg_result_all_int/num_rows], DoubleType()).toDF("neg%")
	count_all = pos_result_all.crossJoin(neg_result_all)

	



	result_days = negResult.select('id', from_unixtime('created_utc').cast(DateType()).alias('date'), 'pos%', 'neg%')
	count_days = result_days.groupby('date').count()

	pos_result_days = result_days.where(result_states['pos%'] == 1).groupby('date').count()
	pos_result_days = pos_result_days.select(col('date').alias('pdate'), col('count').alias('posCount'))
	
	neg_result_days = result_days.where(result_states['neg%'] == 1).groupby('date').count()
	neg_result_days = neg_result_days.select(col('date').alias('ndate'), col('count').alias('negCount'))

	count_days = count_days.join(pos_result_days, pos_result_days.pdate == count_days.date).select('date', 'count', 'posCount')
	count_days = count_days.join(neg_result_days, neg_result_days.ndate == count_days.date).select('date', 'count', 'posCount', 'negCount')
	count_days = count_days.withColumn('pos%', count_days['posCount']/count_days['count'])
	count_days = count_days.withColumn('neg%', count_days['negCount']/count_days['count'])

	



	result_states = negResult.where(col('flair').isin(states))
	count_states = result_states.groupby('flair').count()
	
	pos_result_states = result_states.where(result_states['pos%'] == 1).groupby('flair').count()
	pos_result_states = pos_result_states.select(col('flair').alias('pflair'), col('count').alias('posCount'))
	
	neg_result_states = result_states.where(result_states['neg%'] == 1).groupby('flair').count()
	neg_result_states = neg_result_states.select(col('flair').alias('nflair'), col('count').alias('negCount'))

	count_states = count_states.join(pos_result_states, pos_result_states.pflair == count_states.flair).select('flair', 'count', 'posCount')
	count_states = count_states.join(neg_result_states, neg_result_states.nflair == count_states.flair).select('flair', 'count', 'posCount', 'negCount')
	count_states = count_states.withColumn('pos%', count_states['posCount']/count_states['count'])
	count_states = count_states.withColumn('neg%', count_states['negCount']/count_states['count'])



	# 
	clean_link_id = udf(u_clean_link_id)

	result_story = negResult.select('id', 'pos%', 'neg%', 'title')
	comments_lite = comments.select(col('id').alias('cid'), 'link_id', col('score').alias('cscore'))
	result_story = result_story.join(comments_lite, comments_lite.cid == result_story.id).select('id', 'pos%', 'neg%', 'cscore', clean_link_id('link_id').alias('link_id'))
	submissions_lite = submissions.select(col('id').alias('sid'), 'score')
	
	result_story = result_story.join(submissions_lite, result_story.link_id == submissions_lite.sid).select('id', 'pos%', 'neg%', 'cscore', col('score').alias('sscore'), 'sid')
	
	count_cscore = result_story.groupby('cscore').count()
	
	pos_cscore = result_story.where(result_story['pos%'] == 1).groupby('cscore').count()
	pos_cscore = pos_cscore.select(col('cscore').alias('pcscore'), col('count').alias('posCount'))
	
	neg_cscore = result_story.where(result_story['neg%'] == 1).groupby('cscore').count()
	neg_cscore = neg_cscore.select(col('cscore').alias('ncscore'), col('count').alias('negCount'))

	count_cscore = count_cscore.join(pos_cscore, pos_cscore.pcscore == count_cscore.cscore).select('cscore', 'count', 'posCount')
	count_cscore = count_cscore.join(neg_cscore, neg_cscore.ncscore == count_cscore.cscore).select('cscore', 'count', 'posCount', 'negCount')
	count_cscore = count_cscore.withColumn('pos%', count_cscore['posCount']/count_cscore['count'])
	count_cscore = count_cscore.withColumn('neg%', count_cscore['negCount']/count_cscore['count'])




	count_sscore = result_story.groupby('sscore').count()
	
	pos_sscore = result_story.where(result_story['pos%'] == 1).groupby('sscore').count()
	pos_sscore = pos_sscore.select(col('sscore').alias('psscore'), col('count').alias('posCount'))
	
	neg_sscore = result_story.where(result_story['neg%'] == 1).groupby('sscore').count()
	neg_sscore = neg_sscore.select(col('sscore').alias('nsscore'), col('count').alias('negCount'))

	count_sscore = count_sscore.join(pos_sscore, pos_sscore.psscore == count_sscore.sscore).select('sscore', 'count', 'posCount')
	count_sscore = count_sscore.join(neg_sscore, neg_sscore.nsscore == count_sscore.sscore).select('sscore', 'count', 'posCount', 'negCount')
	count_sscore = count_sscore.withColumn('pos%', count_sscore['posCount']/count_sscore['count'])
	count_sscore = count_sscore.withColumn('neg%', count_sscore['negCount']/count_sscore['count'])

	count_cscore.toPandas().to_csv('count_cscore.csv')

	count_sscore.toPandas().to_csv('count_sscore')

	count_states.toPandas().to_csv('count_states.csv')

	count_days.toPandas().to_csv('count_days.csv')

	count_all.toPandas().to_csv('count_all.csv')


	# Analysis
	count_story = result_story.groupby('sid').count()
	
	pos_story = result_story.where(result_story['pos%'] == 1).groupby('sid').count()
	pos_story = pos_story.select(col('sid').alias('psid'), col('count').alias('posCount'))
	
	neg_story = result_story.where(result_states['neg%'] == 1).groupby('sid').count()
	neg_story = neg_story.select(col('sid').alias('nsid'), col('count').alias('negCount'))

	count_story = count_story.join(pos_story, pos_story.psid == count_story.sid).select('sid', 'count', 'posCount')
	count_story = count_story.join(neg_story, neg_story.nsid == count_story.sid).select('sid', 'count', 'posCount', 'negCount')
	count_story = count_story.withColumn('pos%', count_story['posCount']/count_story['count'])
	count_story = count_story.withColumn('neg%', count_story['negCount']/count_story['count'])

	ten_pos = count_story.where(count_story['pos%'] > 0.9)
	ten_pos = ten_pos.sort(col("pos%").desc())
	ten_neg = count_story.where(count_story['neg%'] > 0.9)
	ten_neg = count_story.sort(col("neg%").desc())
	
if __name__ == "__main__":
	conf = SparkConf().setAppName("CS143 Project 2B")
	conf = conf.setMaster("local[*]")
	sc   = SparkContext(conf=conf)
	sqlContext = SQLContext(sc)
	sc.addPyFile("cleantext.py")
	main(sqlContext)