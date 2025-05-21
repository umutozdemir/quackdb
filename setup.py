from setuptools import setup, find_packages

setup(
    name="quackdb",
    version="0.1.0",
    description="DuckDB wrapper with parquet skipping index based on min/max and outlier aggregates",
    author="Umut Oezdemir",
    python_requires=">=3.8",
    install_requires=[
        "duckdb>=0.8.1",
        "pyarrow>=9.0.0",
    ],
    packages=find_packages(),
    entry_points={
        'console_scripts': [
            'quackdb=quackdb.wrapper:main',
        ],
    },
)