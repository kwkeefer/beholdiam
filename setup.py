from setuptools import setup, find_packages

setup(name='beholdiam',
      version='2.0.0',
      description='beholdiam uses Athena and CloudTrail to audit your IAM resources and generate policy recommendations.',
      url='https://github.com/kwkeefer/beholdiam',
      author='Kyle Keefer',
      author_email='kyle.keefer@protonmail.com',
      license='MIT',
      packages=find_packages(),
      install_requires=[
          'boto3',
          'PyYAML',
          'requests'
      ],
      entry_points={
          'console_scripts': [
              'beholdiam=beholdiam.beholdiam:main'
          ]
      },
      zip_safe=False)
