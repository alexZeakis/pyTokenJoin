from setuptools import setup

setup(
    name="pytokenjoin",
    version="0.1.3",
    description="pyTokenJoin is a library containing efficient algorithms that solve the set similarity join problem with maximum weighted bipartite matching.",
    author="Alexandros Zeakis",
    author_email="azeakis@athenarc.gr",
    url="https://github.com/alexZeakis/pyTokenJoin",
    packages=["pytokenjoin", "pytokenjoin.jaccard", "pytokenjoin.edit", "pytokenjoin.utils"],
    install_requires=[
        "editdistance==0.6.2",
        "networkx==3.1",
        "pandas==1.5.3",
        "numpy==1.23.5"
    ],
)

