from setuptools import setup, find_packages


def readme():
    with open('README.rst') as f:
        return f.read()


setup(name='mlx_flow',
      version='0.1',
      description='flow control',
      url='https://gitlab.fenqi.im/MapleLeafX/ml-x/tree/master/common.flow',
      author='kaibo',
      author_email='zhoukb@fenqi.im',
      license='MIT',
      packages=find_packages(),
      zip_safe=False)
