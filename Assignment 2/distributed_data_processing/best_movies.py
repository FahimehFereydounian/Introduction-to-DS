import mrjob
from collections.abc import Iterator, Iterable
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextValueProtocol, PickleValueProtocol


class BestMovies(MRJob):

    # this pre-processing mapper is for convenience
    # it unpacks the string value into the logical signature of the file
    def map_pre(self, _: None, line: str) -> Iterator[tuple[int, tuple[str, str, float, str, float]]]:
        i, user_id, movie_id, rating, timestamp, rating_normalized = line.split(',')
        yield i, (user_id, movie_id, rating, timestamp, rating_normalized)

    def map_1(self, _: int, line: tuple[str, str, float, str, float]):
        user_id, movie_id, rating, timestamp, rating_normalized = line
        movie_id = int(movie_id)
        rating = float(rating)
        yield movie_id, (rating, 1)

    def reduce_1(self, key, values):
        total_ratings = 0
        count = 0
        for rating, _ in values:
            total_ratings += rating
            count += 1
        yield key, (total_ratings, count)

    def map_2(self, key, value):
        total_ratings, count = value
        if count == 0:
            pass
        avg_rating = total_ratings / count if count > 0 else 0
        if avg_rating >= 4.0 and count >= 10:
            yield None, (key, avg_rating, count)   

    def reduce_2(self, key, values):
        top_movies = sorted(values, key=lambda x: (key, -x[1], -x[2]))[:11]
        for movie in top_movies:
            yield key, movie

    # this post-processing mapper is for convenience
    # the output from the last reducer step is simply written as text into a file, ignoring the key
    def map_post(self, _: None, pair: tuple[str, float]):
        yield None, ','.join(map(str, pair))

    # the output from the last reducer step is simply written as text into a file, ignoring the key
    OUTPUT_PROTOCOL = TextValueProtocol

    # this can be treated as boilerplate
    def steps(self):
        return [MRStep(mapper=self.map_pre),
                MRStep(mapper=self.map_1, reducer=self.reduce_1),
                MRStep(mapper=self.map_2, reducer=self.reduce_2),
                MRStep(mapper=self.map_post)]


if __name__ == '__main__':
    BestMovies.run()
