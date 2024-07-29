# Neovex Database

## Setup 
1. Clone directory
```
git clone git@github.com:LKSeiling/db_neovex.git
```
2. Move ".env" file containing database access information into newly created directory
3. Set local python environment using pyenv (in case pyenv is not installed, see [Installation Guide](https://github.com/pyenv/pyenv?tab=readme-ov-file#installation))
```
pyenv virtualenv 3.10.10 neovex
cd db_neovex # if not already in directory
pyenv local neovex
```
4. Upgrade pip and use it to install the requirments 
```
python3.10 -m pip install --upgrade pip
pip install -r requirements.txt
```
5. Start jupyter notebook server and open the queryDB notebook
```
jupyter notebook queryDB.ipynb
```