from setuptools import setup

FSCHECK_VERSION = 0.1


def setup_package():
    setup(
        name="fscheck",
        version=FSCHECK_VERSION,
        packages=[
            'fscheck'
        ],
        package_data={
        },
        data_files=[
        ],
        #  ----- Requirements -----
        install_requires=[
            'sparkmanager',
            'pyspark==2.2.1',
            'future',
            'docopt',
            'enum34;python_version<"3.4"',
            'numpy',
            'lazy-property',
            'progress'
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
                'fschecksum = fscheck.commands:fsumcheck'
            ]
        }
    )


if __name__ == '__main__':
    setup_package()
