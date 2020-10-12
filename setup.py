import setuptools

setuptools.setup(
    name='p2prpc',
    description='Python package peer to peer remote procedure call',
    version='0.0.1',
    url='https://github.com/Iftimie/P2P-RPC',
    author='Alexandru Iftimie',
    author_email='iftimie.alexandru.florentin@gmail.com',
    packages=setuptools.find_packages(),
    install_requires=[
        'flask',
        'pymongo',
        'multipledispatch',
        'dill==0.3.1.1',
        'varint',
        'mmh3',
        'passlib',
        'requests',
        'deprecated',
        'psutil',
      ],

    keywords=['object', 'detection', 'truck']
    )
