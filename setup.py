from setuptools import setup

setup(
    name='gcloud-tasks-emulator',
    version='0.1',
    description='A stub emulator for the Google Cloud Tasks API',
    url='https://gitlab.com/potato-oss/gcloud-tasks-emulator',
    author='Luke Benstead',
    author_email='lukeb@potatolondon.com',
    license='MIT',
    packages=['gcloud_tasks_emulator'],
    zip_safe=False,
    scripts=[
        "bin/gcloud-tasks-emulator"
    ],
    install_requires=[
        'grpcio',
        'google-cloud-tasks>=1.3.0',
    ]
)

