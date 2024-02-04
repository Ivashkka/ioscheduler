from setuptools import setup, find_packages

VERSION = '0.0.2'
DESCRIPTION = 'Python module that helps to organize input / output operations and async processes'

with open('README.md', 'r') as f:
    desc = f.read()

LONG_DESCRIPTION = desc

setup(
    name="ioscheduler",
    version=VERSION,
    author="Ivashka (Ivan Rakov)",
    author_email="<ivashka.2.r@gmail.com>",
    description=DESCRIPTION,
    packages=find_packages(),
    install_requires=[],
    keywords=['python', 'io', 'input', 'output', 'asyncio', 'threading', 'ioscheduler', 'scheduler'],
    long_description=LONG_DESCRIPTION,
    long_description_content_type="text/markdown",
    platforms='any',
)
