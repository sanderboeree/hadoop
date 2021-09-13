from mrjob.job import MRJob
from mrjob.step import MRStep

class RatingsBreakdown(MRJob):

    def steps(self):
        return [
           MRStep(mapper=self.mapper_get_ratings,
                   reducer=self.reducer_count_ratings),
                   MRStep(reducer=self.reducer_sort_movie)
        ]

    def mapper_get_ratings(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, 1
    
    def reducer_count_ratings (self, key, values):
        yield None, (sum(values), key)

    def reducer_sort_movie(self, _, counts):                                                                
        for count, key in sorted(counts, reverse=True):
            yield (key, int(count))
            
if __name__ == '__main__':
    RatingsBreakdown.run()