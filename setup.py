from setuptools import setup

setup(
    name='btc-py-async-toolbox',
    version='0.0.3',
    packages=['btc'],
    license='MIT',
    author='mtwaro',
    author_email='f20f1a85b3@protonmail.com',
    description='Python toolbox for asynchronous interaction with btc-like nodes.',
    install_requires=['aiohttp', 'pyzmq', 'wheel'],
    long_description=open('README.md').read(),
)
