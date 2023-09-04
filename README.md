# pyTokenJoin

![alt text](img/logo.png)

## Overview

TokenJoin is an efficient method for solving the Fuzzy Set Similarity Join problem. It relies only on tokens and their defined _utilities_, avoiding pairwise comparisons between elements. It is submitted to the International Conference on Very Large Databases (VLDB). This is the repository for the python source code. More information about the original method can be found [here](https://github.com/alexZeakis/TokenJoin).

## Installation

You can easily install `pytokenjoin` from PyPI using `pip`:

```bash
pip install pytokenjoin
```

More on [PyPI](https://pypi.org/project/pytokenjoin/).

## Usage

There are two ways to use TokenJoin:
- When using a threshold δ, e.g. δ=0.7
- When requesting top-k results, e.g. k=100.

There are also two similarity functions supported: Jaccard and Edit Similarity.

More information on how to use the functions can be found on [this jupyter notebook](https://github.com/alexZeakis/pyTokenJoin/blob/main/pytokenjoin/demos/Demo.ipynb). 
