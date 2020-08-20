from setuptools import setup, find_packages


def readme():
    with open('README.rst') as f:
        return f.read()


setup(name='mlx_jdx',
      version='0.1',
      description='jia fen dai',
      url='https://gitlab.fenqi.im/MapleLeafX/ml-x/tree/master/jdx',
      author='weill',
      author_email='weill@fenqi.im',
      license='MIT',
      packages=find_packages(),
      zip_safe=False)
