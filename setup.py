from setuptools import setup

setup(
    name='gapi',
    packages=['gapi'],
    include_package_data=True,
    install_requires=[
        "google_auth_oauthlib",
        "google-api-python-client",
        "pyyaml",
        "jupyter",
        "pandas"
    ]
)
