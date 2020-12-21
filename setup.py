import setuptools

long_description = "None yet"
# with open("README.md", "r", encoding="utf-8") as fh:
#     long_description = fh.read()

setuptools.setup(
    name="sqlitedb",
    version="0.9.3",
    author="Adam Hlavacek",
    author_email="git@adamhlavacek.com",
    description="A thread-safe SQLite database",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/esoadamo/Python-SQLiteDB",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
