import os
import re
from setuptools import setup


def read(path):
    with open(os.path.join(os.path.dirname(__file__), path), 'r') as f:
        data = f.read()
    return data.strip()


_version_re = re.compile(r'\s*__version__\s*=\s*\'(.*)\'\s*')
version = _version_re.findall(read('pefs/__init__.py'))[0]


install_requires = read('requirements.txt').split('\n')
test_requires = read('build-requirements.txt').split('\n')
test_requires.extend(install_requires)

setup(
    name='postgres-efs',
    version=version,
    url='http://github.com/atbentley/postgres-efs/',
    license='MIT',
    author='Andrew Bentley',
    author_email='andrew.t.bentley@gmail.com',
    description='Store Postgres databases on Amazon elastic file systems.',
    long_description=read('README.rst'),
    packages=['pefs'],
    include_package_data=True,
    entry_points='''
        [console_scripts]
        pefs=pefs.cli:cli
    ''',
    zip_safe=False,
    platforms='any',
    install_requires=install_requires,
    tests_require=test_requires,
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.5'
    ]
)


