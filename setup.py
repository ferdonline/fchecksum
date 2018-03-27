from setuptools import setup

FSCHECK_VERSION = 0.1


def setup_package():
    setup(
        name="fchecksum",
        version=FSCHECK_VERSION,
        packages=[
            'fsumcheck'
        ],
        package_data={
        },
        data_files=[
        ],
        #  ----- Requirements -----
        install_requires=[
            'pyspark==2.2.1',
            'sparkmanager',
            'future',
            'docopt'
        ],
        dependency_links=[
            'https://github.com/matz-e/sparkmanager/tarball/master#egg=sparkmanager-0.0.1'
        ],
        setup_requires=[
            'pytest-runner'
        ],
        tests_require=[
            'pytest'
        ],
        extras_require={
            # Dependencies if the user wants a dev env
            'dev': ['flake8']
        },
        entry_points={
            'console_scripts': [
                'fsumcheck = fsumcheck.commands:fsumcheck'
            ]
        }
    )


if __name__ == '__main__':
    setup_package()
