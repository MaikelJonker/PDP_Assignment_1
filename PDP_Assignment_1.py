from mrjob.job import MRJob
from mrjob.step import MRStep

class MovieRatingCounter(MRJob):
	

	# Define order of steps to be taken
	def steps(self):
		return [
			MRStep(mapper=self.mapper_get_ratings,
				combiner=self.combine_ratings,
				reducer=self.reducer_ratings_by_movie),
			MRStep(reducer=self.reducer_test)
			]

	# Define columns and line split character, then we say we only need the movieID
	def mapper_get_ratings(self, _, line):
		(userID, movieID, rating, timestamp) = line.split('\t')
		yield movieID, 1
	
	# Takes the key and the subset, then returns zero of more key-value pairs
	def combine_ratings(self, rating, counts):
		yield rating, sum(counts)
	
	# Sums up the ratings
	def reducer_ratings_by_movie(self, key, values):
		yield None, (sum(values), key)
	
	# Sorts the movies by number of ratings
	def reducer_test(self, _, rating_counts):
		for count, key in sorted(rating_counts, reverse=True):
			yield (key, int(count))

# This line is to excecute the file with the "python" command from console
if __name__ == '__main__':
	MovieRatingCounter.run()
