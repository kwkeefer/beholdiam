from setuptools import setup, find_packages

setup(name='beholdiam',
      version='1.0.3',
      description='Behold uses Athena and CloudTrail to audit your IAM resources and generate policy recommendations.',
      url='https://github.com/kwkeefer/behold',
      author='Kyle Keefer',
      author_email='kyle.keefer@protonmail.com',
      license='MIT',
      packages=find_packages(),
      install_requires=[
          'boto3',
      ],
      entry_points={
          'console_scripts': [
              'behold=behold.behold:main'
          ]
      },
      zip_safe=False)
