from setuptools import find_packages, setup

with open("requirements.txt") as f:
    reqs = f.read().split("\n")

setup(
    version="0.2.0",
    name="dasstore",
    packages=find_packages(),
    include_package_data=True,
    python_requires=">=3.7",
    install_requires=reqs,
    author="Yiyu Ni",
    author_email="niyiyu@uw.edu",
    description="An object storage system for Distributed Acoustic Sensing",
    license="GPL-3.0 license",
    url="https://github.com/niyiyu/dasstore",
)
