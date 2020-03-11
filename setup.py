import os
from setuptools import setup

NAME = "gcloud-tasks-emulator"
PACKAGES = ['gcloud_tasks_emulator']

DESCRIPTION = "A stub emulator for the Google Cloud Tasks API"
URL = "https://gitlab.com/potato-oss/google-cloud/gcloud-tasks-emulator"
LONG_DESCRIPTION = open(os.path.join(os.path.dirname(__file__), 'README.md')).read()

AUTHOR = "Potato London Ltd."
AUTHOR_EMAIL = "mail@p.ota.to"

if os.environ.get('CI_COMMIT_TAG'):
    VERSION = os.environ['CI_COMMIT_TAG']
else:
    VERSION = '0.0.0dev0'


setup(
    name=NAME,
    version=VERSION,
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    long_description_content_type="text/markdown",
    url=URL,
    author=AUTHOR,
    author_email=AUTHOR_EMAIL,
    packages=PACKAGES,
    zip_safe=False,
    keywords=["Google Cloud Tasks", "Google App Engine", "GAE", "GCP"],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
    ],
    scripts=[
        "bin/gcloud-tasks-emulator",
    ],
    install_requires=[
        'grpcio',
        'google-cloud-tasks>=1.3.0',
    ],
    python_requires='>=3.6',
)
