from setuptools import setup, find_packages
import pathlib

here = pathlib.Path(__file__).parent.resolve()

long_description = (here / 'README.rst').read_text(encoding='utf-8')

install_requires = [
    "appdirs==1.4.4",
    "attrs",
    "certifi==2020.6.20",
    "chardet==3.0.4",
    "click==7.1.2",
    "idna==2.10",
    "iniconfig==1.0.1",
    "more-itertools==8.4.0",
    "pathspec==0.8.0",
    "pluggy==0.13.1",
    "py==1.9.0",
    "pyparsing==2.4.7",
    "pytest==6.0.1",
    "regex==2020.7.14",
    "requests==2.24.0",
    "six==1.15.0",
    "toml==0.10.1",
    "typed-ast==1.4.1",
    "urllib3==1.25.10",
    "pyyaml==5.3.1",
    "pdfplumber==0.5.23",
    "pypdf2==1.26.0",
    "pandas==1.1.2",
    "sklearn",
    "numpy",
    "luigi",
]

setup(
    name='swb-covid-dashboard',
    version='1.0.0',
    description='Python project for the swb covid dashboard',
    long_description=long_description,
    long_description_content_type='text/x-rst',
    project_urls={
        'github': 'https://github.com/swb-ief/etl-pipeline'
    },
    packages=find_packages(where='pipeline'),
    install_requires=install_requires,
    extra_requires={
        'dev': ['black', 'jupyter']
    },
    python_requires='>=3.5, <4',
    classifiers=[
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Topic :: System :: Monitoring',
    ],
)
