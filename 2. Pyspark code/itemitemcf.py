# itemitemcf.py
#
# Standalone Python/Spark program to perform Item-Item collaborative filtering 
# to generate recommendations for the products.
# 
# Takes the user book reviews matrix file, created by a hadoop job, as the input, where each line is of the form
# ItemId	userId:rating	userId:rating .... 
# and returns the book recommendations (frequently bought products) for each product.
#
# Usage: spark-submit itemitemcf.py <inputdatafile> <outputdirectory>
# Example usage: spark-submit itemitem.py yxlin.csv /user/abc/output
# 
import sys
import numpy as np
import heapq
from pyspark import SparkContext
from math import*

sc = SparkContext(appName="RecommenderSystem")
if __name__ == "__main__":
  if len(sys.argv) !=3:
    print >> sys.stderr, "Usage: itemitem <datafile> <output directory>"
    exit(-1)

#read the input file and parse it
yxinputFile = sc.textFile(sys.argv[1])
yxinputFile = yxinputFile.map(lambda line: line.split('\t'))
iiMatRDD = yxinputFile.filter(lambda x: len(x) > 1)

# get cartesian product
iiCartRDD = iiMatRDD.cartesian(iiMatRDD)


# remove duplicates that result from cartesian
iiCartRDD = iiCartRDD.filter(lambda x: x[0] != x[1])

# Calculation of Square root used in similarity calculation
def square_rooted(x):
     return round(sqrt(sum([a*a for a in x])),3);

#Calculation of cosine similarity
def cosine_similarity(x,y):
      numerator = sum(a*b for a,b in zip(x,y))
      denominator = square_rooted(x)*square_rooted(y)
      if denominator == 0:
          return 0;
      return round(numerator/denominator, 3);

#calculate similarity of two vectors x and y
def calculate_similarity(x,y):
      identifier_x = x[0]
      vector_x = np.array(map(float,x[1:]))
      identifier_y = x[0]
      vector_y = np.array(map(float,y[1:]))
      return cosine_similarity(vector_x, vector_y);


# compare two vectors; The vectors of the form [userId:rating, ...]
def iicompare(v1, v2):
        i = 1
        j = 1
        v1Mat = [v1[0]]
        v2Mat = [v2[0]]
	# compare the user Ids in the vector and generate two vectors with just the similarity values
        while (i < len(v1)) and (j < len(v2)):
		if (len(v1[i]) <= 1):
			i = i + 1
			continue
		if (len(v2[j]) <= 1):
			j = j + 1
			continue
		#split the value into userId and rating
		item1 = v1[i].split(':')
                item2 = v2[j].split(':')
                if item1[0] == item2[0]:
                        v1Mat = np.append(v1Mat, item1[1])
                        v2Mat = np.append(v2Mat, item2[1])
                        i = i + 1
                        j = j + 1
                elif item1[0] < item2[0]:
                	i = i +1
                else:
                        j = j + 1
	# compute the cosine similarity
        similarity = calculate_similarity(v1Mat, v2Mat);
        return (v1[0], v2[0] + ':' + str(similarity));

# construct a minHeap of similar items for each item and return a list of most similar items
def getPQ(list1):
	heap = []
	similar = []
	for item in list1:
		itemDetails = item.split(':')
		itemId = itemDetails[0]
		sim = float(itemDetails[1])	
		# the minheap size should be 10		
		if len(heap) == 10:
			score = heap[0][0]
			if score < sim and sim != 0:
				heapq.heappop(heap)
				heapq.heappush(heap, (sim, itemId))
		else:
			if sim != 0:
				heapq.heappush(heap, (sim, itemId))
	while heap:
		csScore, item = heapq.heappop(heap)
		similar = np.append(similar, item)
	return similar;

#compare two items
pairRDD = iiCartRDD.map(lambda x: iicompare(x[0], x[1]))

# get the top N similar items
similarityRDD = pairRDD.groupByKey().map(lambda x: (x[0], ' '.join(getPQ(list(x[1])))))

similarityRDD.saveAsTextFile(sys.argv[2])

#print similarityRDD.collect()
