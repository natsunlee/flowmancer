from setuptools import setup, find_packages
from flowmancer.version import __version__

setup(
    name="flowmancer",
    version=__version__,
    author="Nathan Lee",
    author_email="lee.nathan.sh@gmail.com",
    license="MIT",
    python_requires=">=3.6",
    install_requires=[
        "PyYAML>=6.0",
        "pydantic>=1.8.2",
        "tqdm>=4.62.3"
    ],
    packages=find_packages(exclude=["docs", "tests*"]),
    package_data={"flowmancer": ["py.typed"]},
    long_description="Batch development framework."
)