#!/usr/bin/env python
# -*- coding: utf-8 -*-
from pip._internal.req import parse_requirements
from setuptools import find_packages, setup


def load_requirements(fname):
    """Turn requirements.txt into a list"""
    reqs = parse_requirements(fname, session="test")
    return [r.requirement for r in reqs]


setup(
    name="simple_aiokafka",
    description="Simplified API to AIOKafka using pydantic for configuration",
    version="0.0.1",
    url="",
    package_dir={"simple_aiokafka": "simple_aiokafka"},
    packages=find_packages(exclude=["test*"]),
    include_package_data=True,
    install_requires=load_requirements("requirements.txt"),
    python_requires=">=3.9",
)
