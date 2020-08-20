from setuptools import setup, find_packages


def readme():
    with open('README.rst') as f:
        return f.read()


setup(name='ml_graph',
      version='0.1',
      description='ml graph',
      url='https://gitlab.fenqi.im/MapleLeafX/ml-x/tree/master/graph',
      author='weill',
      author_email='weill@fenqi.im',
      license='MIT',
      packages=find_packages(),
      zip_safe=False)
