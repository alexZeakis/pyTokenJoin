from setuptools import setup

setup(
    name="pytokenjoin",
    version="0.1.0",
    description="pyTokenJoin is a library containing efficient algorithms that solve the set similarity join problem with maximum weighted bipartite matching.",
    author="Alexandros Zeakis",
    author_email="azeakis@athenarc.gr",
    url="https://github.com/alexZeakis/pyTokenJoin",
    packages=["pytokenjoin"],
    install_requires=[
        editdistance,
        networkx
    ],
)

