from setuptools import setup

def parse_requirements(filename):
	lines = (line.strip() for line in open(filename))
	return [line for line in lines if line and not line.startswith("#")]

setup(name='TRACA-GENERATOR',
        version='0.1',
        description='Software to generate the inputs for SWAT+ on TRAÇA Project',
        author='Adrià Riu Ruiz',
        author_email='ariu@icra.cat',
        install_requires=parse_requirements('requirements.txt'),
        zip_safe=True)
