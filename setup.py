from setuptools import setup

__version__ = "0.0.2"
__author__ = "Rich Williams"
__author_email__ = (
    "rich@coralgardeners.org"
)
__url__ = "https://github.com/CoralGardeners/reefos-data-api.git"

setup(
    name="reefos-data-api",
    author=__author__,
    author_email=__author_email__,
    version=__version__,
    description="Access Firestore reefos data",
    python_requires=">=3.8",
    packages=["reefos_data_api"],
    include_package_data=True,
    zip_safe=True,
    url=__url__,
    install_requires=[
        "numpy",
        "pandas",
        "scipy",
        "firebase-admin",
    ],
)
