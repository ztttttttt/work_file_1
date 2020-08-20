from setuptools import setup, find_packages


def readme():
    with open('README.rst') as f:
        return f.read()


setup(name='mlx_graph',
      version='0.1',
      description='graph',
      url='https://gitlab.fenqi.im/MapleLeafX/ml-x/tree/master/common.graph',
      author='kaibo',
      author_email='zhoukb@fenqi.im',
      license='MIT',
      packages=find_packages(),
      zip_safe=False)
