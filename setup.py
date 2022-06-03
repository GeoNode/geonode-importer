from setuptools import find_packages, setup

import importer


def read_file(path: str):
    with open(path, "r") as file:
        return file.read()


setup_requires = [
    "wheel",
]

setup(
    name="geonode-importer",
    version=importer.__version__,
    url=importer.__url__,
    description=importer.__doc__,
    long_description=read_file("README.md"),
    author=importer.__author__,
    author_email=importer.__email__,
    platforms="any",
    classifiers=[
        "Environment :: Web Environment",
        "Framework :: Django :: 3.0",
        "License :: OSI Approved :: GNU General Public License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.8",
    ],
    packages=find_packages(),
    package_data={"importer": ["templates/*.html", "templates/layers/*.html"]},
    include_package_data=True,
    install_requires=[
        "dynamic_models @ git+https://github.com/mattiagiupponi/django-dynamic-models@new_settings#egg=dynamic_models",
    ],
)
