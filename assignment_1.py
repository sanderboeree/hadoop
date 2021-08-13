from mrjob.job import MRJob
from mrjob.step import MRStep
import re

WORD_RE = re.compile(r"[\w']+")

class RatingsBreakdown(MRJob):
    def steps(self):
        return [
           MRStep(mapper=self.mapper_get_ratings,
                   reducer=self.reducer_count_ratings),
        ]

    def mapper_get_ratings(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield rating, 1
    
    def reducer_count_ratings (self, key, values):
        yield key, sum(values)


if __name__ == '__main__':
    RatingsBreakdown.run()