# AB-testing
Assetario repository regarding AB testing setup

How to use the repo:

Check out exploration.ipynb notebook to explore empirical distribution and find out what known theoretical disgtribution fits the data the best based on AIC / BIC, graphical evidence and Kolmogorov-Smirnov test.

Run main.py to get the results for client bingo_aloha and lognormal distribution for revenues. There are three steps in the pipeline:

A. Data acquisition - implemented for the Bingo Aloha only, for now.
B. Distribution fit - Bernoulli distribution for the conversions and either exponential or lognormal distribution for the revenues.
C. Produce predictions - probability for the P being the best with the expected losses in both cases for converions and revenue.