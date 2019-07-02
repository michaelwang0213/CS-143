README

The code for computing the top ten lists is at the bottom of reddit_module.py.

Used 2% of comments.
I wrote the the python file such that the number of joins necessary was minimal. For example, the posModel and negModel were run on the same dataframe, with the appropriate aliasing done in the midlle. 

I do not run into any out of memory erros, however, simply using comments.count() ends up taking more than 10 minutes when using only 20% of the comments which I believe is due to the slowness of my computer rather than an error in my code.







