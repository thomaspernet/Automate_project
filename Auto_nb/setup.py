from setuptools import setup

setup(name='auto-nb',
      version='0.1',
      description='Tool to automatically generate Jupyter notebook files',
      url='https://github.com/thomaspernet/Automate_project',
      author='Thomas Pernet',
      scripts=['bin/jupyter-lab-launcher','bin/transfert-knowledge'],
      packages=['auto-nb'],
      install_requires=[
          'nbformat', 'tqdm'
      ],
      zip_safe=False,
      classifiers=[
         "Programming Language :: Python :: 3",
         "License :: OSI Approved :: MIT License",
         "Operating System :: OS Independent",
     ])
