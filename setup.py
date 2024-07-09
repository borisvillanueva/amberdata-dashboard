from setuptools import find_packages, setup

setup(
    name='amberdata_rest',
    packages=find_packages(include=['amberdata_rest']),
    version='0.1.0',
    description='amberdata.io rest api python wrapper',
    author='developer@github.com',
    install_requires=[],
    setup_requires=['pytest-runner'],
    tests_require=['pytest==8.2.2'],
    test_suite='tests',
)