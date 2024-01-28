from setuptools import find_packages, setup

NAME = "yummy"
REQUIRES_PYTHON = ">=3.7.0"

INSTALL_REQUIRE = [
    "click>=7.0.0"
]

YUMMY_MLFLOW_REQUIRE = [ "yummy-mlflow~=0.0.9" ]

YUMMY_DELTA_REQUIRE = [ "yummy-delta~=0.0.9" ]

YUMMY_FEATURES_REQUIRE = [ "yummy-features~=0.0.9" ]

FEAST_REQUIRE = [
    "feast~=0.34.1",
    "polars>=0.13.18",
    "deltalake>=0.5.6",
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

#DEV_REQUIRE = RAY_REQUIRE + \
#    SPARK_REQUIRE + \
#    FEAST_REQUIRE + \
#    YUMMY_MLFLOW_REQUIRE + \
#    YUMMY_DELTA_REQUIRE + \
#    YUMMY_FEATURES_REQUIRE + [
#    "flake8",
#    "black==21.10b0",
#    "isort>=5",
#    "mypy>=0.790",
#    "build>=0.7.0",
#    "twine>=3.4.2",
#    "pytest>=6.0.0",
#]

DEV_REQUIRE = [
    "scikit-learn>=1.0.0",  
    "filelock>=3.0.0"
    "flake8>=6.0.0,<6.1.0",
    "black>=22.6.0,<23",
    "isort>=5,<6",
    "mypy>=0.981,<0.990",
    "build>=0.7.0",
    "twine>=3.4.2",
    "pytest>=6.0.0,<8",
]


setup(
    name=NAME,
    version="0.0.11",
    author="Qooba",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    python_requires=REQUIRES_PYTHON,
    url="https://github.com/yummyml/yummy",
    packages=find_packages(exclude=["test*", "feature_repo*"]),
    install_requires=INSTALL_REQUIRE,
    extras_require={
        "delta": YUMMY_DELTA_REQUIRE,
        "features": YUMMY_FEATURES_REQUIRE,
        "mlflow": YUMMY_MLFLOW_REQUIRE,
        "feast": FEAST_REQUIRE,
        "dask": DASK_REQUIRE,
        "ray": RAY_REQUIRE,
        "spark": SPARK_REQUIRE,
        "dev": DEV_REQUIRE,
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
