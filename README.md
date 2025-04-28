# NEOVEX Database

## Setup 
1. Clone directory
```
git clone git@github.com:LKSeiling/db_neovex.git
```
2. Move ".env" file containing database access information into newly created directory (in case you want to import data, you need to adapt the BASE_PATH value in the .env file to point to the relevant base directory)
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

## Usage
### Wiki
Please take a look at the **[Wiki](https://github.com/LKSeiling/db_neovex/wiki)** to see the [API documentation](https://github.com/LKSeiling/db_neovex/wiki/API) as well as an [explanation of the examples]() which you can try yourself by running
```
jupyter notebook queryDB_examples.ipynb
```

The Wiki also includes information on the [structure of the underlying database](https://github.com/LKSeiling/db_neovex/wiki/Structure-of-NEOVEX-Database), which is relevant in case you want to crafting your own SQL queries.

### Make your own queries
To make your own queries, start a jupyter notebook server and open the queryDB notebook using
```
jupyter notebook queryDB.ipynb
```
