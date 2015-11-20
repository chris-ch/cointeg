import os
from setuptools import setup


def read(fname):
    """
    Utility function to read the README file.
    Used for the long_description.  It's nice, because now 1) we have a top level README file and 2) it's easier to
    type in the README file than to put a raw string in below ...
    :param fname:
    :return:
    """
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name='stat arb toolbox',
    version='0.1',
    author='Christophe',
    author_email='chris.perso@gmail.com',
    description='investigating mean reversion on ETFs.',
    license='BSD',
    keywords='mean reversion systematic trading',
    packages=['statsmodelsext', 'mktdata', 'mktdatadb', 'statsext', 'bollinger'],
    long_description=read('README.md'),
    install_requires=[
        'pandas', 'pytz', 'numpy', 'statsmodels', 'matplotlib', 'Quandl', 'scipy', 'xlsxwriter'],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Topic :: Utilities',
        'License :: OSI Approved :: BSD License',
    ],
)