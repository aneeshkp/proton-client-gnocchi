from setuptools import setup

setup(name='proton-client-gnocci',
      version='0.1',
      description='',
      url='https://github.com/aneeshkp/proton-client-gnocchi.git',
      author='Aneesh Puttur',
      author_email='aneeshputtur@gmail.com',
      license='apache',
      packages=['protonclientgnocchi'],
      install_requires=[
          'python-qpid-proton==0.8.2'
      ],
      zip_safe=False)
