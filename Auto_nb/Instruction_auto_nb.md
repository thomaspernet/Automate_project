# Instruction Workflow

## Installation guide



1.  Download the folder from GitHub

Make sure you are in the download folder.


```
cd /Users/Thomas/Downloads
svn export https://github.com/thomaspernet/Automate_project/trunk/Auto_nb
```


then change the current working directory and install the program


```
cd "/Users/Thomas/Downloads/Auto_nb"

python setup.py install
```

After the installation is done, you can remove the folder from the download folder

```
rm -r /Users/Thomas/Downloads/Auto_nb
```

###  Uninstall
```


pip uninstall auto-nb --y
```
