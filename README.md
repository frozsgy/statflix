# statflix

term project for ceng790 big data analysis course.
aims to analyze user interests by country and recommend content for users.

## part 1: understanding users by country 

### available content

netflix content availability changes by country.
can we understand user interest in a genre by looking at how many shows with that genre are available in a country?
spoiler: no. netflix aims to make all content globally available.

implemented in `TitlesInUnogs.scala`

### produced content

can we understand user interest in a genre by looking at how many shows with that genre are produced by a country?
spoiler: partially. since producing investments are decided via extensive market research.

implemented in `TitlesInUnogs.scala`

### weekly popular content

well, lets just analyze the genres of weekly popular content by each country, compare the results with available and produced content results.

implemented in `TitlesInWeeklyLists.scala`

## part 2: recommendation

no user information, so instead lets use everything else we have.

implemented in `ContentBasedRecommendation.scala`
