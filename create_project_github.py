import os, shutil, re

project_name = 'ATest'
path_template = \
"/Users/thomas/Google Drive/Projects/Data_science/GitHub/" \
"Template_project_Github"
path_destination = "/Users/thomas/Google Drive/Projects/Data_science/GitHub/" \
"Repositories"
new_path = os.path.join(path_destination, project_name)
os.mkdir(path=new_path)

def copytree(src, dst, symlinks=False, ignore=None):
    for item in os.listdir(src):
        s = os.path.join(src, item)
        d = os.path.join(dst, item)
        if os.path.isdir(s):
            shutil.copytree(s, d, symlinks, ignore)
        else:
            shutil.copy2(s, d)

copytree(path_template,new_path)

### Rename project
with open(os.path.join(new_path, 'README.md'), "r") as f:
    lines = f.read()

lines = lines.replace('PROJECTNAME',
                          project_name)

with open(os.path.join(new_path, 'README.md'), 'w') as file:
    file.write(lines)

os.chdir(new_path)

### Config the hub file
### https://hub.github.com/ -> for mac
####https://github.com/github/hub/issues/1067#issuecomment-482133220

os.system("git init")
os.system("git add .")
os.system('git commit -m "Create proejct"')
### Create repo github
os.system('hub create')
### Oush to GitHub
push ="git remote add origin https://github.com/thomaspernet/{}".format(
project_name)
os.system(push)
os.system("git push -u origin master")
