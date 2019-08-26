import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="behold",
    version="0.0.1",
    author="Kyle Keefer",
    author_email="author@example.com",
    description="An AWS IAM auditing tool.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/kwkeefer/behold",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)