from setuptools import setup, find_packages


def readme():
    with open('README.rst') as f:
        return f.read()


setup(name='mlx_configparser',
      version='0.1',
      description='',
      url='https://gitlab.fenqi.im/MapleLeafX/ml-x-config/tree/master/configparser',
      author='lian',
      author_email='lia@fenqi.im',
      license='MIT',
      packages=find_packages(),
      zip_safe=False)
