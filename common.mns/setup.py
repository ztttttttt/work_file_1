from setuptools import setup, find_packages


def readme():
    with open('README.rst') as f:
        return f.read()


setup(name='mns',
      version='0.1',
      description='',
      url='https://gitlab.fenqi.im/MapleLeafX/ml-x/tree/master/common.mns',
      author='lian',
      author_email='lia@fenqi.im',
      license='MIT',
      packages=find_packages(),
      zip_safe=False)