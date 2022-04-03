from pathlib import Path
from setuptools import setup, find_packages


def get_version() -> str:
    version = dict()  # type: ignore
    with open(Path(__file__).parent / "flowmancer" / "version.py") as f:
        exec(f.read(), version)
    return version['__version__']


setup(
    name="flowmancer",
    version=get_version(),
    author="Nathan Lee",
    author_email="lee.nathan.sh@gmail.com",
    license="MIT",
    python_requires=">=3.6",
    install_requires=["PyYAML==5.4.1", "pyaml-env==1.1.4", "pydantic==1.9.0", "tqdm==4.62.3", "rich==11.2.0"],
    packages=find_packages(exclude=["docs", "tests*"]),
    package_data={"flowmancer": ["py.typed"]},
    long_description=open("README.md").read(),
    long_description_content_type='text/markdown',
    entry_points={"console_scripts": ["flowmancer=flowmancer.cli:main"]},
    classifiers=[
        "Intended Audience :: Developers",
        "Programming Language :: Python :: >=3.6",
    ],
)
