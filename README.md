### Local setup and installation using miniconda
1) Install [miniconda](https://docs.conda.io/en/latest/miniconda.html)

2) Create a conda environment and install the code dependencies from the `env.yml`. Run the following command in the root directory of the repo.
    ```
    $ conda env create -f env.yml
    ```

    Alternatively, you can pip install from `requirements.txt`

    ```
    $ conda create -n dope -c conda-forge python=3.9 -y
    $ pip install -r requirements.txt 
    ```
3) Using the virtual environment:

    ```
    $ conda activate dope
    ```
    activates the virtual environment.

    ```
    $ conda deactivate dope
    ``` 
    exits the virtual environment.

