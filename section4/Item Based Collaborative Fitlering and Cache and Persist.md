==================================
Item based Collaborative Filtering
==================================

Scenario:
Suggest movies to person A based on the preferences/votes of other people that 
watched similar movies to person A

E.G
- Find every movie pairs: movieA,movieB rated by the same person
- Measure similarties of rates across users that watch the same movies
- sort based on simlairty strength

E.G.
Bob likes Movie A
Bob likes Movie B
Susie likes Movie A
Susie likes Movie B
Charlie likes Movie A
--> based on Bob and Susie rates, we can recommend Movie B

=================================================
Item based Collaborative Filtering wiht Pyspark
=================================================
- select userId,movieId and ratings columns
- generate all the pairs for movieA, movieB for movies watched by the same person
  and include related ratings: (movieA,movieB, ratingA,ratingB)
- filter out duplicates (e.g. movieA,movieB is the same as movieB,movieA)  
- compute cosine similarity scores for every pair
  - add x^2, y^2 and xy columns
  - group by (movieA,movieB) pairs (we have an entry per person)
    use as group function the similarity score 
- filter, sort and display results      


=================================================
Caching Datasets
=================================================
If you are using the dataset more than once, you should cache it
or spark will run all the steps toc reate it, again
Use:
- cache (only in memory)
- persist (can save to a filesystem)

