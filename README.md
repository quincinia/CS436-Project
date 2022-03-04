# CS436-Project

Ean Jacob Gayban, Andrew Hervey, Charles Pezeshki

Uses data from [here](https://netsg.cs.sfu.ca/youtubedata/) and provides the following operations:

- Top-k queries
  - Show top k categories by # of videos
  - Show top k rated videos
  - Show top k most popular videos (views)
- Range queries
  - Given a list of categories C, and a range [t1, t2], find all videos that match
- User recommendations
  - Given a username, show all recommended videos that appear under that user's videos

Running instructions for main.py:

"spark-submit main.py queryChoice fileName Args"
