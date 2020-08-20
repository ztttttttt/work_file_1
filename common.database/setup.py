from setuptools import setup, find_packages


def readme():
    with open('README.rst') as f:
        return f.read()


setup(name='mlx_database',
      version='0.1',
      description='',
      url='https://gitlab.fenqi.im/MapleLeafX/ml-x/tree/master/common.database',
      author='zhoukb',
      author_email='zhoukb@fenqi.im',
      license='MIT',
      packages=find_packages(),
      zip_safe=False)
