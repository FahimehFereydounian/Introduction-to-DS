import mrjob
from collections.abc import Iterator, Iterable
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextValueProtocol


class Haters(MRJob):


    def mapper(self, _, line: str):
        i, user_id, movie_id, rating, timestamp, rating_normalized = line.split(',')
        yield user_id, (float(rating), 1)


    def reducer(self, key, values):
        negative_ratings = 0
        for rating, x in values:
            if rating < 2.0:
                negative_ratings += x
        if negative_ratings >= 50:
            yield None, key



if __name__ == '__main__':
    Haters.run()
