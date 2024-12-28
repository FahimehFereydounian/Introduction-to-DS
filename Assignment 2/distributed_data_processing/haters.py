import mrjob
from collections.abc import Iterator, Iterable
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextValueProtocol


class Haters(MRJob):


    def mapper(self, _, line: str):
        i, user_id, movie_id, rating, timestamp, rating_normalized = line.split(',')
        # TODO
        ...

    def reducer(self, key, values):
        # TODO
        ...


if __name__ == '__main__':
    Haters.run()
