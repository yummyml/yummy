from setuptools import find_packages, setup

NAME = "yummy"
REQUIRES_PYTHON = ">=3.7.0"

INSTALL_REQUIRE = [
    "feast~=0.22.1",
    "polars>=0.13.18",
    "yummy-rs~=0.0.5",
]

DASK_REQUIRE = [
    "dask[distributed]>=2021.11.0",
]

RAY_REQUIRE = DASK_REQUIRE + [
    "ray[default,data]>=1.9.1",
]

SPARK_REQUIRE = [
    "pyspark>=3.0.0",
]

DELTA_REQUIRE = [
    "deltalake>=0.5.6",
]

DEV_REQUIRE = RAY_REQUIRE + \
    SPARK_REQUIRE + \
    DELTA_REQUIRE + [
    "flake8",
    "black==21.10b0",
    "isort>=5",
    "mypy>=0.790",
    "build>=0.7.0",
    "twine>=3.4.2",
    "pytest>=6.0.0",
]

setup(
    name=NAME,
    version="0.0.5",
    author="Qooba",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    python_requires=REQUIRES_PYTHON,
    url="https://github.com/yummyml/yummy",
    packages=find_packages(exclude=["test*", "feature_repo*"]),
    install_requires=INSTALL_REQUIRE,
    extras_require={
        "dev": DEV_REQUIRE,
        "dask": DASK_REQUIRE,
        "ray": RAY_REQUIRE,
        "spark": SPARK_REQUIRE,
        "delta": DELTA_REQUIRE,
    },
    keywords=("feast featurestore polars dask ray pyspark offlinestore"),
    license='Apache License, Version 2.0',
    classifiers=[
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
    entry_points={"console_scripts": ["yummy=yummy.cli:cli"]},
)
