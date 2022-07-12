import sys

try:
    from setuptools import setup, find_packages
    use_setuptools = True
except ImportError:
    from distutils.core import setup
    use_setuptools = False

try:
    with open('README.rst', 'rt') as readme:
        description = '\n' + readme.read()
except IOError:
    # maybe running setup.py from some other dir
    description = ''

python_requires = '>=3.6'
install_requires = [
    'Rx>=3.2.0',
]

setup(
    name="rxx",
    version='0.2.1',
    url='https://github.com/maki-nage/rxx.git',
    license='MIT',
    description="ReactiveX extensions",
    long_description=description,
    author='Romain Picard',
    author_email='romain.picard@oakbits.com',
    packages=find_packages(),
    install_requires=install_requires,
    platforms='any',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: MIT License',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3',
    ],
    project_urls={
        'Documentation': 'https://www.makinage.org/doc/rxx/latest/index.html',
    }
)
